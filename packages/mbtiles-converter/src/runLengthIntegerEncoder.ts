export default class RunLengthIntegerEncoder {
    /*
    * -> Literals
    *   -> Header -> 0x80 to 0xff -> corresponds to the negative of number of literals in the sequence
    *   -> Varint encoded values
    * -> Runs
    *   -> Header -> 0x00 to 0x7f -> encodes the length of the run - 3
    *   -> Varint encoded value
    *
    * */

    //TODO: use ByteBuffer and return byte array
    public static encode(values: number[]): Uint8Array[] {
        const encodedValuesBuffer: Uint8Array[] = [];
        let literals: Array<number> = [];
        let runs: Array<number> = [];
        for (let i = 0; i < values.length; i++) {
            const value = values[i];

            if (literals.length == 0x7f) {
                this.addLiteralsToBuffer(encodedValuesBuffer, literals);
                literals = [];
            }
            /* if there is a sequence of 127 runs or a literal sequence begins store the runs */
            else if (runs.length == 0x7f || (runs.length >= 3 && values[i - 1] != value)) {
                this.addRunsToBuffer(encodedValuesBuffer, runs);
                runs = [];
            }
            /* store literals and transfer to runs*/
            else if (literals.length >= 2 && values[i - 2] == value && values[i - 1] == value && runs.length == 0) {
                const numLiterals = literals.length - 2;
                if (numLiterals > 0) {
                    this.addLiteralsToBuffer(encodedValuesBuffer, literals.slice(0, numLiterals));
                }
                literals = [];

                runs.push(values[i - 2]);
                runs.push(values[i - 1]);
            }

            if (i == values.length - 1) {
                if (runs.length > 1) {
                    runs.push(value);
                    this.addRunsToBuffer(encodedValuesBuffer, runs);
                    runs = [];
                } else {
                    literals.push(value);
                    this.addLiteralsToBuffer(encodedValuesBuffer, literals);
                    literals = [];
                }
            } else if (runs.length > 0) {
                runs.push(value);
            } else {
                literals.push(value);
            }
        }

        return encodedValuesBuffer;
    }

    private static addLiteralsToBuffer(buffer: Uint8Array[], literals: Array<number>) {
        /**
         * Literals start with an initial byte of 0x80 to 0xff,
         * which corresponds to the negative of number of literals in the sequence.
         * Following the header byte, the list of N varints is encoded
         */
        const header = new Uint8Array(1);
        header[0] = 256 - literals.length
        buffer.push(header);
        const encodedValues: number[] = literals.map(k => k);
        const varintEncodedLiterals = this.varintEncode(encodedValues);
        buffer.push(varintEncodedLiterals);
    }

    private static addRunsToBuffer(buffer: Uint8Array[], runs: Array<number>) {
        /**
         * Runs start with an initial byte of 0x00 to 0x7f, which encodes the length of the run - 3.
         * The value of the run is encoded as a base 128 varint.
         * */
        const header = new Uint8Array(1);
        header[0] = runs.length - 3;
        buffer.push(header);
        const varintEncodedRun: Uint8Array = this.varintEncode([runs[0]]);
        buffer.push(varintEncodedRun);
    }

    private static varintEncode(values: number[]): Uint8Array {
        const varintBuffer = new Uint8Array(values.length * 4);
        let i = 0;
        for (const value of values) {
            i = this.putVarInt(value, varintBuffer, i);
        }
        return varintBuffer.slice(0, i);
    }

    private static putVarInt(v: number, sink: Uint8Array, offset: number) {
        do {
            // Encode next 7 bits + terminator bit
            const bits: number = v & 0x7F;
            v >>>= 7;
            const b: number = (bits + ((v !== 0) ? 0x80 : 0)) as number;
            sink[offset++] = b;
        } while (v !== 0);
        return offset;
    }

    public static decode(rleEncodedBuffer, numValues: number): number[] {
        console.log("buffer", rleEncodedBuffer);
        const decodedValues = [];
        decodedValues.length = numValues
        let valuesCounter = 0;
        for (let i = 0; i < rleEncodedBuffer.length;) {
            const header: number = rleEncodedBuffer[i] & 0xFF;
            i++;
            /* runs start with an initial byte of 0x00 to 0x7f */
            if (header <= 0x7f) {
                const numRuns: number = header + 3;

                const varInt = this.decodeVarIntRuns(rleEncodedBuffer, i);
                i = varInt[0];
                for (let j = 0; j < numRuns; j++) {
                    decodedValues[valuesCounter++] = varInt[1];
                }
            } else {
                /* Literals start with an initial byte of 0x80 to 0xff, which corresponds to the negative of number of literals in the sequence */
                const numLiterals: number = 256 - header;

                for (let j = 0; j < numLiterals; j++) {
                    i = this.decodeVarIntLiterals(rleEncodedBuffer, i, decodedValues, valuesCounter++);
                }
            }
        }

        return decodedValues;
    }

    public static decodeVarIntRuns(src: number[], offset: number) {
        let result = 0;
        let shift = 0;
        let b: number;
        do {
            // Get 7 bits from next byte
            b = src[offset++];
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return [offset, result];
    }

    public static decodeVarIntLiterals(src: number[], offset: number, dst?: number[], dstOffset?: number) {
        let result = 0;
        let shift = 0;
        let b: number;
        do {
            // Get 7 bits from next byte
            b = src[offset++];
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        dst[dstOffset] = result;
        return offset
    }
}