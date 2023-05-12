#!/usr/bin/env node
import fs from "fs";
import { program } from "commander";
import { ComtIndex }from "@com-tiles/provider";
import { Metadata } from "@com-tiles/spec";
import { MBTilesRepository } from "./mbTilesRepository";
import { toBytesLE } from "./utils.js";
import pkg from "../package.json";
import MapTileProvider, { RecordType } from "./tileProvider";
import Logger from "./logger";
import RunLengthIntegerEncoder from "./runLengthIntegerEncoder";

program
    .version(pkg.version)
    .option("-i, --inputFilePath <path>", "specify path and filename of the MBTiles database")
    .option("-o, --outputFilePath <path>", "specify path and filename of the COMT archive file")
    .option(
        "-m, --maxZoomDbQuery <number>",
        "specify to which zoom level the TileMatrixLimits should be queried from the db and not calculated based on the bounds",
    )
    .option(
        "-z, --maxPyramidZoom <number>",
        "specify to which zoom level the compressed pyramid is fetched.",
    )
    .parse(process.argv);

const options = program.opts();
if (!options.inputFilePath || !options.outputFilePath) {
    throw new Error("Please specify the inputFilePath and the outputFilePath.");
}

const MAGIC = "COMT";
const VERSION = 1;
const INDEX_ENTRY_SIZE_BYTE_LENGTH = 4;
const MAX_ZOOM_DB_QUERY = parseInt(options.maxZoomDbQuery) || 7;
const MAX_ZOOM_OF_PYRAMID = parseInt(options.maxPyramidZoom) || 7;


(async () => {
    const logger = new Logger();
    const mbTilesFilename = options.inputFilePath;
    const comTilesFilename = options.outputFilePath;

    logger.info(`Converting the MBTiles file ${mbTilesFilename} to a COMTiles archive.`);
    await createComTileArchive(mbTilesFilename, comTilesFilename, logger);
    logger.info(`Successfully saved the COMTiles archive in ${comTilesFilename}.`);
})();

async function createComTileArchive(mbTilesFilename: string, comTilesFileName: string, logger: Logger) {
    const repo = await MBTilesRepository.create(mbTilesFilename);
    const metadata = await repo.getMetadata(MAX_ZOOM_OF_PYRAMID);
    /**
     * Query the TileMatrixLimits for the lower zoom levels from the db.
     * It's common to have more tiles as an overview in the lower zoom levels then specified in the bounding box of the MBTiles metadata.
     * This is a trade off because querying the limits for all zoom levels from the db takes very long.
     */
    const filteredTileMatrixSet = metadata.tileMatrixSet.tileMatrix.filter(
        (tileMatrix) => tileMatrix.zoom <= MAX_ZOOM_DB_QUERY,
    );
    for (const tileMatrix of filteredTileMatrixSet) {
        tileMatrix.tileMatrixLimits = await repo.getTileMatrixLimits(tileMatrix.zoom);
    }

    const tileMatrix = metadata.tileMatrixSet.tileMatrix;
    const metadataJson = JSON.stringify(metadata);

    let stream = fs.createWriteStream(comTilesFileName, {
        encoding: "binary",
        highWaterMark: 1_000_000,
    });

//Structure:
    // Header | Metadata | Encoded Pyramid Index Length | Encoded Fragment Index Length | Data
    logger.info("Writing the header and metadata to the COMTiles archive.");
    writeHeader(stream, metadataJson.length);
    writeMetadata(stream, metadataJson);

    const tileProvider = new MapTileProvider(repo, tileMatrix);
    logger.info("Writing the index to the COMTiles archive.");
    logger.log("metadataOffset", metadata.tileOffsetBytes);
    const indexEntryByteLength = INDEX_ENTRY_SIZE_BYTE_LENGTH + metadata.tileOffsetBytes;

    const pyramidIndexByteLength = await writePyramidIndex(stream, tileProvider, metadata, indexEntryByteLength);

    // const fragmentsIndexByteLength = await writeFragmentIndex(stream, tileProvider, metadata, indexEntryByteLength);

    logger.info("Writing the map tiles to the the COMTiles archive.");
    await writeTiles(stream, tileProvider);

    stream.end();
    await repo.dispose();

    stream = fs.createWriteStream(comTilesFileName, { flags: "r+", start: 12 });
    const pyramidIndexLengthBuffer = toBytesLE(pyramidIndexByteLength, 5);
    stream.write(pyramidIndexLengthBuffer);
    // const fragmentsIndexLengthBuffer = toBytesLE(fragmentsIndexByteLength, 5);
    // stream.write(fragmentsIndexLengthBuffer);
    stream.end();
}

function writeHeader(stream: fs.WriteStream, metadataByteLength: number) {
    //Structure of the header:
    //  Magic (char[4]) | Version (4 bytes) | Metadata Length (4 Bytes) | Index Pyramid Length (5 Bytes) | Index Fragment Length (5 Bytes)
    stream.write(MAGIC);

    const versionBuffer = Buffer.alloc(4);
    versionBuffer.writeUInt32LE(VERSION);
    stream.write(versionBuffer);

    const metadataLengthBuffer = Buffer.alloc(4);
    metadataLengthBuffer.writeUInt32LE(metadataByteLength);
    stream.write(metadataLengthBuffer);
    // Write PyramidIndexLength and FragmentsIndexLength
    //start index 12
    const pyramidIndexLengthBuffer = toBytesLE(0, 5);
    stream.write(pyramidIndexLengthBuffer);
    //start index 17
    // const fragmentsIndexLengthBuffer = toBytesLE(0, 5);
    // stream.write(fragmentsIndexLengthBuffer);
    /* Reserve the bytes but write the real index size later when the full index is written to disk.
     *  Calculating the index size results in a false size */
    // const indexLengthBuffer = toBytesLE(0, 5);
    // stream.write(indexLengthBuffer);
}

function writeMetadata(stream: fs.WriteStream, metadataJson: string) {
    stream.write(metadataJson, "utf-8");
}

async function writePyramidIndex(
    stream: fs.WriteStream,
    tileProvider: MapTileProvider,
    metadata: Metadata,
    indexEntryByteLength: number,
) {
    let pyramidIndexEntries = 0;
    const maxZoom = 7;
    const encodedPyramidTiles = await getEncodedPyramidTileEntries(metadata, tileProvider, maxZoom);
    for (const encodedPyramidTile of encodedPyramidTiles) {
        await writeIndexEntry(stream, 0, encodedPyramidTile);
        pyramidIndexEntries += encodedPyramidTile.byteLength;
    }
    return pyramidIndexEntries * indexEntryByteLength;
}

async function writeFragmentIndex(
    stream: fs.WriteStream,
    tileProvider: MapTileProvider,
    metadata: Metadata,
    indexEntryByteLength: number,
) {
    let fragmentIndexEntries = 0;
    const maxZoom = MAX_ZOOM_OF_PYRAMID || 7;
    const encodedFragmentTiles = await getEncodedFragmentTileEntries(metadata, tileProvider, maxZoom);
    for (const Fragment of encodedFragmentTiles) {
        await writeIndexEntry(stream, 0, Fragment);
        fragmentIndexEntries += Fragment.byteLength;
    }
    return fragmentIndexEntries * indexEntryByteLength;
}
async function getEncodedPyramidTileEntries (
    metadata: Metadata,
    tileProvider: MapTileProvider,
    maxZoom: number
) {
    const comtIndex = new ComtIndex(metadata);
    const tileSizes = [];
    const previousIndex = -1;
    const tiles = tileProvider.getTilesInRowMajorOrder(RecordType.SIZE);
    for await (const { zoom, column, row  , size: tileLength } of tiles) {
        if(zoom >= 0 && zoom <= maxZoom) {
            const { index: currentIndex } = comtIndex.calculateIndexOffsetForTile(zoom, column, row);
            const padding = currentIndex - previousIndex - 1;
            // if (padding > 0) {
            //     for (let i = 0; i < padding; i++) {
            //         tileSizes.push(0);
            //     }
            // }
            tileSizes.push(tileLength);
        }
    }
    const encodedValues = RunLengthIntegerEncoder.encode(tileSizes);
    console.log("pyramidTileSizes", tileSizes.length);
    return encodedValues;
}
async function getEncodedFragmentTileEntries (
    metadata: Metadata,
    tileProvider: MapTileProvider,
    maxZoom: number
) {
    const comtIndex = new ComtIndex(metadata);
    const tileSizes = [];
    const tiles = tileProvider.getTilesInRowMajorOrder(RecordType.SIZE);
    const previousIndex = -1;
    for await (const { zoom, column, row  , size: tileLength } of tiles) {
        if(zoom > maxZoom) {
            const { index: currentIndex } = comtIndex.calculateIndexOffsetForTile(zoom, column, row);
            const padding = currentIndex - previousIndex - 1;
            // if (padding > 0) {
            //     for (let i = 0; i < padding; i++) {
            //         tileSizes.push(0);
            //     }
            // }
            tileSizes.push(tileLength);
        }
    }
    const encodedValues = RunLengthIntegerEncoder.encode(tileSizes);
    console.log("fragmentTileSizes", tileSizes.length);
    return encodedValues;
}

async function writeIndexEntry(
    stream: fs.WriteStream,
    offset: number,
    encodedLength: Uint8Array
): Promise<void> {
    const tileBuffer = Buffer.from(encodedLength);
    await writeBuffer(stream, tileBuffer);
}
async function writeTiles(stream: fs.WriteStream, tileProvider: MapTileProvider): Promise<void> {
    const tiles = tileProvider.getTilesInRowMajorOrder(RecordType.TILE);

    /* Batching the tile writes did not bring the expected performance gain because allocating the buffer
     * for the tile batches was to expensive. So we simplified again to single tile writes. */
    for await (const { data: tile } of tiles) {
        const tileLength = tile.byteLength;
        if (tileLength > 0) {
            const tileBuffer = Buffer.from(tile);
            await writeBuffer(stream, tileBuffer);
        }
    }
}

async function writeBuffer(stream: fs.WriteStream, buffer: Buffer): Promise<void> {
    const canContinue = stream.write(buffer);
    if (!canContinue) {
        await new Promise((resolve) => {
            stream.once("drain", resolve);
        });
    }
}
