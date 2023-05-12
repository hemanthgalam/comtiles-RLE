export default class Logger {
    trace(message: string): void {
        console.trace(message);
    }

    info(message: string): void {
        console.info(message);
    }

    log(message: string, value: any): void {
        console.log(`${message}`, value);
    }
}
