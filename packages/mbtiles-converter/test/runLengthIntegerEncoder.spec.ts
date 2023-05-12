import RunLengthIntegerEncoder from "../src/runLengthIntegerEncoder";

describe("Encode runs and literals", () => {
    it("should encode literals with header & runs of size >=3 with header", () => {
        const values = [1, 10, 4, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7];


        const encodedLiteralsHeader = new Uint8Array([251]);
        const encodedLiterals = new Uint8Array([1, 10, 4, 6, 6]);
        const encodedRunsHeader = new Uint8Array([7]);
        const encodedRuns = new Uint8Array([7]);
        const encodedValuesBuffer: Uint8Array[] = [encodedLiteralsHeader, encodedLiterals, encodedRunsHeader, encodedRuns];
        const encodedValues = RunLengthIntegerEncoder.encode(values);
        expect(encodedValues).toStrictEqual(encodedValuesBuffer);
    });

    it("should encode runs and literals with a sequence of 'runs,literals,runs,runs'", () => {
        const values = [2, 2, 2, 2, 8, 1, 5, 10, 9, 4, 4, 4, 4, 4, 4, 6, 6, 6, 6];
        const encodedLiteralsHeader = new Uint8Array([251]);
        const encodedLiterals = new Uint8Array([ 8, 1, 5, 10, 9 ]);
        const encodedRuns1Header = new Uint8Array([1]);
        const encodedRuns2Header = new Uint8Array([ 3 ]);
        const encodedRuns3Header = new Uint8Array([ 1 ]);
        const encodedRuns1 = new Uint8Array([ 2 ]);
        const encodedRuns2 = new Uint8Array([ 4 ]);
        const encodedRuns3 = new Uint8Array( [ 6 ]);
        const encodedValuesBuffer: Uint8Array[] = [
            encodedRuns1Header, encodedRuns1,
            encodedLiteralsHeader, encodedLiterals,
            encodedRuns2Header, encodedRuns2,
            encodedRuns3Header, encodedRuns3
        ];
        const encodedValues = RunLengthIntegerEncoder.encode(values);
        expect(encodedValues).toStrictEqual(encodedValuesBuffer);
    });
});
