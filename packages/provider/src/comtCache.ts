import pako from "pako";
import { Metadata } from "@com-tiles/spec";
import ComtIndex, { FragmentRange } from "./comtIndex";
import LruCache from "./lruCache";
import { convertUInt40LEToNumber, Optional } from "./utils";
import BatchRequestDispatcher, { TileRequest } from "./batchRequestDispatcher";
import { TmsIndex, XyzIndex } from "./tileIndex";
import CancellationToken from "./cancellationToken";
import RunLengthIntegerEncoder from "../../mbtiles-converter/src/runLengthIntegerEncoder";

interface Header {
    pyramidIndexOffset: number;
    // fragmentIndexOffset: number;
    pyramidIndexLength: number;
    // fragmentIndexLength: number;
    dataOffset: number;
    metadata: Metadata;
    partialIndex: ArrayBuffer;

    pyramidEntries: number[];
}

interface IndexEntry {
    offset?: number;
    size: number;
}

class IndexCache {
    private static readonly INDEX_ENTRY_NUM_BYTES = 9;
    /* 7 zoom levels (8-14) * 4 fragments per zoom */
    private static readonly MAX_ENTRIES_LRU_CACHE = 28;
    private readonly fragmentedIndex = new LruCache<number, { fragmentRange: FragmentRange; indexEntries: Uint8Array }>(
        IndexCache.MAX_ENTRIES_LRU_CACHE,
    );
    private readonly comtIndex: ComtIndex;

    /*
     * The partial index is always kept in memory and can be mixed up with fragmented and unfragmented tile matrices.
     * For the index fragments which are added to the cache a LRU cache is used.
     * Through this procedure there can be redundant index entries in the partial index and the LRU cache
     * when the last fragment of the partial index is incomplete, but in general this doesn't matter.
     * */
    constructor(
        private readonly metadata: Metadata,
        private readonly partialIndex = new Uint8Array(0),
        private readonly pyramidEntries = [],
        private readonly pyramidIndexLength: number,
        // private readonly fragmentIndexLength: number,
        private readonly cacheSize?: number,
    ) {
        this.comtIndex = new ComtIndex(this.metadata);
    }

    setIndexFragment(fragmentRange: FragmentRange, indexEntries: Uint8Array): void {
        const index = fragmentRange.startOffset;
        this.fragmentedIndex.put(index, { fragmentRange, indexEntries });
    }

    /**
     * @param tmsIndex Index of a specific tile in the TMS tiling scheme.
     * @returns Relative offset and size of the specified tile in the data section.
     */
    get(tmsIndex: TmsIndex): IndexEntry {
        const { z, x, y } = tmsIndex;
        const { index } = this.comtIndex.calculateIndexOffsetForTile(z, x, y);
        const { startOffset, index: fragmentStartIndex } = this.comtIndex.getFragmentRangeForTile(z, x, y);
        console.log("indexOffset for Tile", index);
        console.log("startOffset", startOffset);
        console.log("partialIndexByteLength", this.partialIndex.byteLength)
        const indexOffset = index;
        if (indexOffset <= this.partialIndex.byteLength ) {
            return this.createIndexEntry(indexOffset, this.pyramidEntries);
        }

        const indexFragment = this.fragmentedIndex.get(startOffset);
        if (!indexFragment) {
            return null;
        }

        const relativeFragmentOffset = (index - fragmentStartIndex) * IndexCache.INDEX_ENTRY_NUM_BYTES;
        return this.createIndexEntry(relativeFragmentOffset, indexFragment.indexEntries);
    }

    private createIndexEntry(indexOffset: number, indexEntries): IndexEntry {
        console.log("writing index")
        const indexBuffer = indexEntries;
        console.log("indexBuffer", indexBuffer)
        const offsetEntries = indexEntries.slice(0, indexOffset);
        const offset = offsetEntries.reduce((a, b) => a + b, 0);
        const size = indexBuffer[indexOffset];
        console.log("SIZE", size);
        return { offset, size };
    }
}

//index entries start index 0 ^ zoom level 1 ^  offset will be dataentryoffset + index entry size at zoom level

export enum HeaderFetchStrategy {
    PREFETCH = "PREFETCH",
    LAZY = "LAZY ",
}

/*
 * The ComtCache class has currently the following limitations regarding the support of the COMTiles spec:
 * - The only supported TileMatrixCRS is WebMercatorQuad
 * - Only Mapbox vector tiles are supported as content of a map tile and no raster formats (PNG, WebP)
 * - The only supported space-filling curve type for the order of the index fragments and tiles is row-major
 * - Only index fragments can be loaded after the initial fetch.
 *   So with the first initial fetch all the unfragmented part of the index has to be fetched and can't be lazy loaded.
 * */
export default class ComtCache {
    private static readonly SUPPORTED_VERSION = 1;
    //todo change to 25 kb
    private static readonly INITIAL_CHUNK_SIZE = 2.56 * 10 ** 4;
    private static readonly METADATA_OFFSET_INDEX = 17;
    private static readonly NEW_METADATA_OFFSET_INDEX = 17;
    private static readonly SUPPORTED_TILE_MATRIX_CRS = "WebMercatorQuad";
    private static readonly SUPPORTED_ORDERING = "RowMajor";
    private static readonly INDEX_ENTRY_NUM_BYTES = 9;
    private indexCache: IndexCache = null;
    private comtIndex: ComtIndex = null;
    private readonly requestCache = new Map<number, Promise<ArrayBuffer>>();
    private headerLoaded: Promise<Header>;
    private readonly batchedTilesProvider: BatchRequestDispatcher;

    private constructor(private readonly comtUrl: string, private readonly throttleTime, private header?: Header) {
        if (header) {
            this.initIndex(header);
        }

        this.batchedTilesProvider = new BatchRequestDispatcher(comtUrl, throttleTime);
    }

    /**
     * @param comtUrl Url to object storage where the COMTiles archive is hosted.
     * @param prefetchHeader Specifies if the header should be prefetched or lazy loaded.
     * @param throttleTime Time to wait for batching up the tile requests.
     */
    static async create(
        comtUrl: string,
        prefetchHeader = HeaderFetchStrategy.PREFETCH,
        throttleTime = 5,
    ): Promise<ComtCache> {
        const header = prefetchHeader === HeaderFetchStrategy.PREFETCH ? await ComtCache.loadHeader(comtUrl) : null;
        return new ComtCache(comtUrl, throttleTime, header);
    }

    /**
     * @param comtUrl Url to object storage where the COMTiles archive is hosted.
     * @param throttleTime Time to wait for batching up the tile requests.
     */
    static createSync(comtUrl: string, throttleTime = 5): ComtCache {
        return new ComtCache(comtUrl, throttleTime);
    }

    /**
     * Fetches a map tile with the given XYZ index from the specified COMTiles archive.
     *
     * @param xyzIndex Index of the tile in the XYZ tiling scheme.
     * @param cancellationToken For aborting the tile request.
     */
    async getTile(xyzIndex: XyzIndex, cancellationToken?: CancellationToken): Promise<ArrayBuffer> {
        const provider = (index, indexEntry, absoluteTileOffset, cancellationToken) => {
            return this.fetchMVT(absoluteTileOffset, indexEntry.size, cancellationToken);
        };

        return this.fetchTile(xyzIndex, provider, cancellationToken);
    }

    /**
     * Tries to batch the tile requests by aggregating all requests within the given throttleTime.
     *
     * @param xyzIndex Index of the tile in the XYZ tiling scheme.
     * @param cancellationToken For aborting the tile request.
     */
    async getTileWithBatchRequest(xyzIndex: XyzIndex, cancellationToken?: CancellationToken): Promise<ArrayBuffer> {
        const provider = (index, indexEntry, absoluteTileOffset, cancellationToken) => {
            const tileRequest: TileRequest = {
                index: xyzIndex,
                range: {
                    startOffset: absoluteTileOffset,
                    endOffset: absoluteTileOffset + indexEntry.size - 1,
                },
            };

            return this.batchedTilesProvider.fetchTile(tileRequest, cancellationToken);
        };

        return this.fetchTile(xyzIndex, provider, cancellationToken);
    }

    private async fetchTile(
        xyzIndex: XyzIndex,
        tileProvider: (
            index: XyzIndex,
            indexEntry: IndexEntry,
            absoluteTileOffset: number,
            cancellationToken: CancellationToken,
        ) => Promise<ArrayBuffer>,
        cancellationToken?: CancellationToken,
    ): Promise<ArrayBuffer> {
        const optionalIndexEntry = await this.getIndexEntry(xyzIndex, cancellationToken);
        if (!optionalIndexEntry.isPresent()) {
            return new Uint8Array(0);
        }
        const { indexEntry, absoluteTileOffset } = optionalIndexEntry.get();

        /* Return an empty array if the tile is missing */
        return indexEntry.size
            ? tileProvider(xyzIndex, indexEntry, absoluteTileOffset, cancellationToken)
            : new Uint8Array(0);
    }

    private async getIndexEntry(
        xyzIndex: XyzIndex,
        cancellationToken,
    ): Promise<Optional<{ indexEntry: IndexEntry; absoluteTileOffset: number }>> {
        /* Lazy load the header on the first tile request */
        if (!this.header) {
            if (!this.headerLoaded) {
                this.headerLoaded = ComtCache.loadHeader(this.comtUrl);
                this.header = await this.headerLoaded;
                this.initIndex(this.header);
            } else {
                await this.headerLoaded;
            }
        }

        const { metadata } = this.header;
        const { x, y, z } = xyzIndex;
        /* COMTiles uses the y-axis alignment of the TMS spec which is flipped compared to xyz */
        const tmsY = (1 << z) - y - 1;
        const limit = metadata.tileMatrixSet.tileMatrix[z].tileMatrixLimits;
        if (x < limit.minTileCol || x > limit.maxTileCol || tmsY < limit.minTileRow || tmsY > limit.maxTileRow) {
            /* Requested tile not within the boundary ot the TileSet */
            return Optional.empty();
        }

        const tmsIndex = { z, x, y: tmsY };
        const indexEntry = this.indexCache.get(tmsIndex) ?? (await this.fetchIndexEntry(tmsIndex, cancellationToken));
        console.log("tms Index", tmsIndex);
        console.log("indexEntry",indexEntry);
        const absoluteTileOffset = this.header.dataOffset + indexEntry.offset;
        console.log("absoluteTileOffset",absoluteTileOffset)

        return Optional.of({ indexEntry, absoluteTileOffset });
    }

    private async fetchIndexEntry(tmsIndex: TmsIndex, cancellationToken: CancellationToken): Promise<IndexEntry> {
        const fragmentRange = this.comtIndex.getFragmentRangeForTile(tmsIndex.z, tmsIndex.x, tmsIndex.y);
        console.log("fragmentRange", fragmentRange);
        let indexFragment: ArrayBuffer;
        /* avoid redundant requests to the same index fragment */
        if (!this.requestCache.has(fragmentRange.startOffset)) {
            const startOffset = this.header.pyramidIndexOffset + fragmentRange.startOffset;
            const endOffset = this.header.pyramidIndexOffset + fragmentRange.endOffset;
            const indexEntryRequest = ComtCache.fetchBinaryData(
                this.comtUrl,
                startOffset,
                endOffset,
                cancellationToken,
            );
            this.requestCache.set(fragmentRange.startOffset, indexEntryRequest);

            try {
                indexFragment = await indexEntryRequest;
            } finally {
                this.requestCache.delete(fragmentRange.startOffset);
            }
        } else {
            indexFragment = await this.requestCache.get(fragmentRange.startOffset);
        }

        this.indexCache.setIndexFragment(fragmentRange, new Uint8Array(indexFragment));
        return this.indexCache.get(tmsIndex);
    }

    private initIndex(header: Header): void {
        this.indexCache = new IndexCache(
            this.header.metadata,
            new Uint8Array(this.header.partialIndex),
            this.header.pyramidEntries,
            this.header.pyramidIndexLength
        );
        this.comtIndex = new ComtIndex(header.metadata);
    }

    private static fetchHeader(comtUrl: string): Promise<ArrayBuffer> {
        return ComtCache.fetchBinaryData(comtUrl, 0, ComtCache.INITIAL_CHUNK_SIZE - 1);
    }

    private static async fetchBinaryData(
        url: string,
        firstBytePos: number,
        lastBytePos: number,
        cancellationToken?: CancellationToken,
    ): Promise<ArrayBuffer> {
        const requestOptions = {
            headers: {
                range: `bytes=${firstBytePos}-${lastBytePos}`,
            },
        };

        let abortRequest;
        if (cancellationToken) {
            const controller = new AbortController();
            const { signal, abort } = controller;
            Object.assign(requestOptions, { signal });
            abortRequest = abort.bind(controller);
            cancellationToken.register(abortRequest);
        }

        const response = await fetch(url, requestOptions);
        cancellationToken?.unregister(abortRequest);

        if (!response.ok) {
            throw new Error(response.statusText);
        }

        return response.arrayBuffer();
    }

    private static async loadHeader(comtUrl: string): Promise<Header> {
        const buffer = await ComtCache.fetchHeader(comtUrl);

        const view = new DataView(buffer);

        const version = view.getUint32(4, true); //version
        if (version !== ComtCache.SUPPORTED_VERSION) {
            throw new Error("The specified version of the COMT archive is not supported.");
        }

        const metadataSize = view.getUint32(8, true); //metaSize
        const pyramidIndexLength = view.getUint32(12, true);
        // const fragmentIndexLength = view.getUint32(17, true);

        // const indexOffset = ComtCache.NEW_METADATA_OFFSET_INDEX + metadataSize; //IE
        // DataOffset =  pyramidIndexOffset + fragmentsIndexOffset
        console.warn("pyramidIndexLength", pyramidIndexLength);
        // console.warn("fragmentIndexLength", fragmentIndexLength);
        console.warn("metadataSize", metadataSize);
        const pyramidIndexOffset = ComtCache.NEW_METADATA_OFFSET_INDEX + metadataSize; //23 + DATA SIZE = START OF PE
        // const fragmentIndexOffset = pyramidIndexLength + ComtCache.NEW_METADATA_OFFSET_INDEX + metadataSize; //23 + DATA SIZE = START OF PE
        const metadataBuffer = buffer.slice(ComtCache.NEW_METADATA_OFFSET_INDEX, pyramidIndexOffset);
        const metadataDocument = new TextDecoder().decode(metadataBuffer);
        const metadata = JSON.parse(metadataDocument);
        console.warn("metadata", metadata);
        const dataOffset = pyramidIndexOffset + (pyramidIndexLength / ComtCache.INDEX_ENTRY_NUM_BYTES);
        console.warn("dataOffset", dataOffset);
        //Fetch decoded Pyramid Index Entries
        const decodedPyramidValues = this.fetchPyramidIndex(buffer, dataOffset, pyramidIndexOffset);
        //Fetch decoded Fragment Index Entries
        // const decodedFragmentValues = this.fetchFragmentIndex(buffer, dataOffset, fragmentIndexOffset)
        // console.warn("fragments", decodedFragmentValues);

        const numCompleteIndexEntries = Math.floor(
            (ComtCache.INITIAL_CHUNK_SIZE - pyramidIndexOffset) / ComtCache.INDEX_ENTRY_NUM_BYTES,
        );

        console.log("numCompleteIndexEntries", numCompleteIndexEntries);
        // this.validateMetadata(metadata, numCompleteIndexEntries);

        /* truncate last potential incomplete IndexEntry */
        const endOffset = pyramidIndexOffset + numCompleteIndexEntries * ComtCache.INDEX_ENTRY_NUM_BYTES;
        // partial index should contain only index pyramid entries which are decoded
        const partialIndex = buffer.slice(pyramidIndexOffset, endOffset);
        const pyramidEntries = decodedPyramidValues
        // const partialIndex = buffer.slice(pyramidIndexOffset, fragmentIndexOffset); // need to know need of partial index
        console.log("endOffset", endOffset);
        // const partialIndex  = new Uint8Array(decodedPyramidValues).buffer
        console.log("partial index Array Buffer", partialIndex);
        return {
            pyramidIndexOffset,
            pyramidIndexLength,
            dataOffset,
            metadata,
            partialIndex,
            pyramidEntries
        };
    }

    private static async loadMetaData(buffer, pyramidIndexOffset) {
        const metadataBuffer = buffer.slice(ComtCache.NEW_METADATA_OFFSET_INDEX, pyramidIndexOffset);
        const metadataDocument = new TextDecoder().decode(metadataBuffer);
        const metadata = JSON.parse(metadataDocument);
        return metadata
    }

    private static fetchPyramidIndex(buffer, dataOffset, pyramidIndexOffset) {
        const pyramidIndexEntriesBuffer = buffer.slice(pyramidIndexOffset, dataOffset);
        const pyramidIndexData = new DataView(pyramidIndexEntriesBuffer);
        const pyramidIndexEntries = []
        const decodedPyramidValues = []
        for(let i=0; i<pyramidIndexData.byteLength; i++) {
            pyramidIndexEntries.push(pyramidIndexData.getUint8(i));
        }
        const decodedPyramidBuffer = RunLengthIntegerEncoder.decode(pyramidIndexEntries, pyramidIndexEntries.length);
        console.warn("decodedPyramidBuffer", decodedPyramidBuffer.length);
        for(let i=0; i <= decodedPyramidBuffer.length; i++) {
            const tileSize = decodedPyramidBuffer[i];
            // if(tileSize > 0) {
                decodedPyramidValues.push(tileSize);
            // }
        }
        console.warn("decodedPyramidValues", decodedPyramidValues.length);
        return decodedPyramidValues;
    }
    private static fetchFragmentIndex(buffer, dataOffset, fragmentIndexOffset) {
        const fragmentIndexEntriesBuffer = buffer.slice(fragmentIndexOffset, dataOffset);
        const fragmentIndexData = new DataView(fragmentIndexEntriesBuffer);
        const fragmentIndexEntries = [];
        const decodedFragmentValues = [];
        for(let i=0; i<fragmentIndexData.byteLength; i++) {
            fragmentIndexEntries.push(fragmentIndexData.getUint8(i));
        }
        const decodedFragmentIndexBuffer = RunLengthIntegerEncoder.decode(fragmentIndexEntries, fragmentIndexEntries.length);
        for(let i=0; i <= decodedFragmentIndexBuffer.length; i++) {
            const tileSize = decodedFragmentIndexBuffer[i];
            if(tileSize > 0) {
                decodedFragmentValues.push(tileSize);
            }
        }
        return decodedFragmentValues;
    }

    private static validateMetadata(metadata: Metadata, numOfPyramidIndexEntries: number): void {
        if (metadata.tileFormat !== "pbf") {
            throw new Error("Currently pbf (MapboxVectorTiles) is the only supported tileFormat.");
        }

        const tileMatrixSet = metadata.tileMatrixSet;
        const supportedOrdering = [undefined, ComtCache.SUPPORTED_ORDERING];
        if (
            ![tileMatrixSet.fragmentOrdering, tileMatrixSet.tileOrdering].every((ordering) =>
                supportedOrdering.some((o) => o === ordering),
            )
        ) {
            throw new Error(`The only supported fragment and tile ordering is ${ComtCache.SUPPORTED_ORDERING}`);
        }

        if (
            tileMatrixSet.tileMatrixCRS !== undefined &&
            tileMatrixSet?.tileMatrixCRS.trim().toLowerCase() !== ComtCache.SUPPORTED_TILE_MATRIX_CRS.toLowerCase()
        ) {
            throw new Error(`The only supported TileMatrixCRS is ${ComtCache.SUPPORTED_TILE_MATRIX_CRS}.`);
        }
        console.log("tileMatrixSet",tileMatrixSet);
        const unfragmentedIndexEntries = tileMatrixSet.tileMatrix
            .filter((tm) => tm.aggregationCoefficient === -1)
            .reduce((numIndexEntries, tm) => {
                const limits = tm.tileMatrixLimits;
                console.log("numIndexEntries",numIndexEntries);
                return (
                    numIndexEntries +
                    (limits.maxTileCol - limits.minTileCol + 1) * (limits.maxTileRow - limits.minTileRow + 1)
                );
            }, 0);
        console.log("unfragmentedpart", unfragmentedIndexEntries);
        /* Currently only index fragments can be loaded after the initial fetch */
        if (unfragmentedIndexEntries > numOfPyramidIndexEntries) {
            throw new Error(
                "The unfragmented part (aggregationCoefficient=-1) of the index has to be part of the initial fetch. Only index fragments can be reloaded",
            );
        }
    }

    private async fetchMVT(
        tileOffset: number,
        tileSize: number,
        cancellationToken: CancellationToken,
    ): Promise<Uint8Array> {
        const buffer = await ComtCache.fetchBinaryData(
            this.comtUrl,
            tileOffset,
            tileOffset + tileSize - 1,
            cancellationToken,
        );
        const compressedTile = new Uint8Array(buffer);
        return pako.ungzip(compressedTile);
    }
}
