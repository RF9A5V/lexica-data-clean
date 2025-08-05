/**
 * Disk Monitoring Script for LEXIS LANTERN
 * 
 * This script monitors available disk space and sends alerts if space drops
 * below threshold. Runs on a schedule for nightly reporting.
 */

import fs from 'fs/promises';
import { execSync } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

// For ES modules, we need to create the equivalent of __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Define base directory to monitor
const BASE_DIR = path.join(__dirname, '../data/lexis_lantern');
const ALERT_THRESHOLD_GB = 50; // Alert if free space drops below this value
const CRITICAL_THRESHOLD_GB = 20; // Critical alert threshold

/**
 * Get free disk space on Windows
 */
function getFreeDiskSpace() {
    try {
        // Get drive letter from path
        const driveLetter = BASE_DIR.split(path.sep)[0];
        
        // Run PowerShell command to get disk info
        const command = `powershell -command "(Get-PSDrive ${driveLetter}).Free/1GB"`;
        const output = execSync(command).toString().trim();
        
        // Parse the result
        const freeSpaceGB = parseFloat(output);
        return { freeSpaceGB, error: null };
    } catch (error) {
        return { freeSpaceGB: null, error: error.message };
    }
}

/**
 * Get size of directory in GB
 */
async function getDirectorySize(directoryPath) {
    try {
        // Run PowerShell command to get directory size
        const command = `powershell -command "'{0:N2}' -f ((Get-ChildItem -Recurse '${directoryPath}' | Measure-Object -Property Length -Sum).Sum / 1GB)"`;
        const output = execSync(command).toString().trim();
        
        // Parse the result
        const sizeGB = parseFloat(output.replace(',', '.'));
        return { sizeGB, error: null };
    } catch (error) {
        return { sizeGB: null, error: error.message };
    }
}

/**
 * Generate report
 */
async function generateReport() {
    const timestamp = new Date().toISOString();
    const { freeSpaceGB, error: diskError } = getFreeDiskSpace();
    let status = 'OK';
    let message = '';
    
    if (diskError) {
        status = 'ERROR';
        message = `Could not determine free disk space: ${diskError}`;
    } else if (freeSpaceGB < CRITICAL_THRESHOLD_GB) {
        status = 'CRITICAL';
        message = `CRITICAL: Only ${freeSpaceGB.toFixed(2)} GB free space remaining!`;
    } else if (freeSpaceGB < ALERT_THRESHOLD_GB) {
        status = 'WARNING';
        message = `WARNING: Only ${freeSpaceGB.toFixed(2)} GB free space remaining.`;
    } else {
        message = `${freeSpaceGB.toFixed(2)} GB free space available.`;
    }
    
    // Get size of LEXIS LANTERN data
    const { sizeGB: dataSizeGB, error: sizeError } = await getDirectorySize(BASE_DIR);
    let dataSize = '';
    
    if (sizeError) {
        dataSize = `Could not determine data size: ${sizeError}`;
    } else {
        dataSize = `LEXIS LANTERN data size: ${dataSizeGB.toFixed(2)} GB`;
    }
    
    // Generate Windsurf report format
    const report = [
        `==== DISK MONITOR REPORT ====`,
        `TIMESTAMP: ${timestamp}`,
        `STATUS: ${status}`,
        `FREE SPACE: ${freeSpaceGB ? freeSpaceGB.toFixed(2) + ' GB' : 'UNKNOWN'}`,
        `${dataSize}`,
        `MESSAGE: ${message}`,
        `=====================`
    ].join('\n');
    
    // Log to console (would send to Windsurf in real implementation)
    console.log(report);
    
    // For demo purposes, write to a log file
    try {
        const logPath = path.join(__dirname, '../data/lexis_lantern/disk_monitor.log');
        let existingLog = '';
        
        try {
            existingLog = await fs.readFile(logPath, 'utf8');
        } catch (err) {
            // File doesn't exist yet, that's fine
        }
        
        await fs.writeFile(logPath, report + '\n\n' + existingLog);
    } catch (err) {
        console.error('Error writing log file:', err.message);
    }
    
    // Return the report data
    return {
        timestamp,
        status,
        freeSpaceGB: freeSpaceGB ? freeSpaceGB.toFixed(2) : null,
        dataSizeGB: dataSizeGB ? dataSizeGB.toFixed(2) : null,
        message
    };
}

/**
 * Main execution function
 */
async function main() {
    try {
        const report = await generateReport();
        
        // Send alert if necessary (simulated)
        if (report.status === 'WARNING' || report.status === 'CRITICAL') {
            console.log(`\nWARNING: Would send Windsurf priority ping to @ProjectLead`);
            console.log(`Message: ${report.message}`);
        }
        
        return report;
    } catch (error) {
        console.error('Error in disk monitoring:', error);
        process.exit(1);
    }
}

// Execute the script if run directly
if (import.meta.url === `file://${__filename}`) {
    main();
}

export { getFreeDiskSpace, getDirectorySize, generateReport };
