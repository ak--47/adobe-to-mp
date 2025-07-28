import bunyan from 'bunyan';
import { LoggingBunyan } from '@google-cloud/logging-bunyan';
import bunyanFormat from 'bunyan-format';

const NODE_ENV = process.env.NODE_ENV || "unknown";

const loggerStreams = [];

// Add pretty-printing stream for local development
if (NODE_ENV === 'dev') {
    loggerStreams.push({
        stream: bunyanFormat({ outputMode: 'short', color: true }),
        level: 'debug' // Show all debug messages in dev
    });
} else {
    // In production, always log to stdout (Cloud Functions capture this)
    loggerStreams.push({
        stream: process.stdout,
        level: 'info'
    });
}

// Additionally add Cloud Logging stream for production
if (NODE_ENV !== 'dev') {
    try {
        const loggingBunyanStream = new LoggingBunyan({ 
            logName: 'adobe-transform', 
            redirectToStdout: true  // This is key for Cloud Functions
        });
        loggerStreams.push(loggingBunyanStream.stream('info'));
    } catch (err) {
        console.error('Failed to initialize Cloud Logging:', err);
    }
}

export const log = bunyan.createLogger({
    name: 'adobe-transform',
    streams: loggerStreams
});

export default log;