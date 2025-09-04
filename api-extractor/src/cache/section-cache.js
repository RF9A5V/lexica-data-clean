import fs from 'fs/promises';
import path from 'path';
import { ensureDir } from '../lib/utils.js';

/**
 * Section cache manager for storing and retrieving NYSenate API section data
 */
export class SectionCache {
  constructor(cacheDir = 'data/cache') {
    this.cacheDir = cacheDir;
  }

  /**
   * Get cache file path for a law
   */
  getCacheFilePath(lawId) {
    return path.join(this.cacheDir, `${lawId.toLowerCase()}-sections.json`);
  }

  /**
   * Check if cached sections exist for a law
   */
  async hasCachedSections(lawId) {
    try {
      const cacheFile = this.getCacheFilePath(lawId);
      await fs.access(cacheFile);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Load cached sections for a law
   */
  async loadCachedSections(lawId) {
    try {
      const cacheFile = this.getCacheFilePath(lawId);
      const content = await fs.readFile(cacheFile, 'utf8');
      const data = JSON.parse(content);
      
      // Validate cache structure
      if (!data.lawId || !data.sections || !Array.isArray(data.sections)) {
        throw new Error('Invalid cache file structure');
      }
      
      return data;
    } catch (error) {
      throw new Error(`Failed to load cached sections for ${lawId}: ${error.message}`);
    }
  }

  /**
   * Save sections to cache
   */
  async saveSectionsToCache(lawId, sections, metadata = {}) {
    try {
      await ensureDir(this.cacheDir);
      
      const cacheData = {
        lawId,
        cachedAt: new Date().toISOString(),
        sectionsCount: sections.length,
        metadata,
        sections
      };
      
      const cacheFile = this.getCacheFilePath(lawId);
      await fs.writeFile(cacheFile, JSON.stringify(cacheData, null, 2), 'utf8');
      
      console.log(`  ✅ Cached ${sections.length} sections for law ${lawId}`);
      return cacheFile;
    } catch (error) {
      throw new Error(`Failed to save sections to cache for ${lawId}: ${error.message}`);
    }
  }

  /**
   * Get cache statistics
   */
  async getCacheStats() {
    try {
      await ensureDir(this.cacheDir);
      const files = await fs.readdir(this.cacheDir);
      const cacheFiles = files.filter(f => f.endsWith('-sections.json'));
      
      const stats = {
        totalCachedLaws: cacheFiles.length,
        laws: []
      };
      
      for (const file of cacheFiles) {
        try {
          const content = await fs.readFile(path.join(this.cacheDir, file), 'utf8');
          const data = JSON.parse(content);
          stats.laws.push({
            lawId: data.lawId,
            sectionsCount: data.sectionsCount,
            cachedAt: data.cachedAt
          });
        } catch (error) {
          console.warn(`Warning: Invalid cache file ${file}: ${error.message}`);
        }
      }
      
      return stats;
    } catch (error) {
      return { totalCachedLaws: 0, laws: [], error: error.message };
    }
  }

  /**
   * Clear cache for a specific law
   */
  async clearLawCache(lawId) {
    try {
      const cacheFile = this.getCacheFilePath(lawId);
      await fs.unlink(cacheFile);
      console.log(`  ✅ Cleared cache for law ${lawId}`);
    } catch (error) {
      if (error.code !== 'ENOENT') {
        throw new Error(`Failed to clear cache for ${lawId}: ${error.message}`);
      }
    }
  }

  /**
   * Clear all cached sections
   */
  async clearAllCache() {
    try {
      await ensureDir(this.cacheDir);
      const files = await fs.readdir(this.cacheDir);
      const cacheFiles = files.filter(f => f.endsWith('-sections.json'));
      
      for (const file of cacheFiles) {
        await fs.unlink(path.join(this.cacheDir, file));
      }
      
      console.log(`  ✅ Cleared ${cacheFiles.length} cached law files`);
    } catch (error) {
      throw new Error(`Failed to clear cache: ${error.message}`);
    }
  }
}
