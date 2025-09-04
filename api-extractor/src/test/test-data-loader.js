/**
 * Test Data Loader
 * Loads pre-fetched test data for testing without API calls
 */

const fs = require('fs/promises');
const path = require('path');

class TestDataLoader {
  constructor() {
    this.dataDir = path.join(__dirname, '..', '..', 'test-data', 'sections');
  }

  /**
   * Load all test sections from pre-fetched data
   */
  async loadAllSections() {
    try {
      const allSectionsFile = path.join(this.dataDir, 'all-sections.json');
      const data = await fs.readFile(allSectionsFile, 'utf8');
      return JSON.parse(data);
    } catch (error) {
      console.warn('Could not load pre-fetched test data. Run: npm run fetch-test-data');
      return this.getFallbackTestData();
    }
  }

  /**
   * Load a specific section by law ID and section number
   */
  async loadSection(lawId, sectionNum) {
    try {
      const filename = `${lawId.toLowerCase()}-${sectionNum.replace(/\./g, '_')}.json`;
      const filepath = path.join(this.dataDir, filename);
      const data = await fs.readFile(filepath, 'utf8');
      return JSON.parse(data);
    } catch (error) {
      console.warn(`Could not load section ${lawId} § ${sectionNum}. Run: npm run fetch-test-data`);
      return this.getFallbackSectionData(lawId, sectionNum);
    }
  }

  /**
   * Get fallback test data for when pre-fetched data is not available
   */
  getFallbackTestData() {
    return [
      {
        lawId: 'ABC',
        sectionNum: '3',
        description: 'Complex definitions section with many subsections',
        text: `§ 3. Definitions. As used in this chapter:
1. "Alcoholic beverage" means any liquid containing alcohol.
2. "Beer" means fermented beverages from malt.
3-a. "Biomass feedstock" means any substance.
(a) Application rules apply.
(i) Primary requirements must be met.
(A) Documentation is needed.
(1) Form specifications are required.`,
        title: 'Definitions',
        success: true,
        isFallback: true
      },
      {
        lawId: 'PEN',
        sectionNum: '60.35',
        description: 'Decimal section numbering',
        text: `§ 60.35 Authorized disposition; murder in the first degree offenders; aggravated murder offenders.
1. Indeterminate sentence. When a person is to be sentenced upon conviction for the offense of murder in the first degree as defined in section 125.27 of this chapter or for the offense of aggravated murder as defined in section 125.26 of this chapter, and the defendant was eighteen years of age or older at the time of the commission of the crime, the court shall sentence the defendant to life imprisonment without parole in accordance with subdivision five of section 70.00 of this chapter, unless the defendant was under eighteen years of age at the time of the commission of the crime in which case the defendant shall be sentenced in accordance with section 70.05 of this chapter.`,
        title: 'Authorized disposition; murder in the first degree offenders; aggravated murder offenders',
        success: true,
        isFallback: true
      }
    ];
  }

  /**
   * Get fallback data for a specific section
   */
  getFallbackSectionData(lawId, sectionNum) {
    const fallbackData = this.getFallbackTestData();
    const section = fallbackData.find(s => s.lawId === lawId && s.sectionNum === sectionNum);
    
    if (section) {
      return section;
    }
    
    return {
      lawId,
      sectionNum,
      text: `§ ${sectionNum}. Sample section text for testing.
1. First subsection with sample content.
2. Second subsection with more content.
(a) Paragraph under subsection 2.
(i) Subparagraph content.`,
      title: `Sample Section ${sectionNum}`,
      success: true,
      isFallback: true
    };
  }

  /**
   * Check if test data directory exists and has data
   */
  async hasTestData() {
    try {
      const allSectionsFile = path.join(this.dataDir, 'all-sections.json');
      await fs.access(allSectionsFile);
      return true;
    } catch (error) {
      return false;
    }
  }

  /**
   * Get statistics about available test data
   */
  async getTestDataStats() {
    try {
      const files = await fs.readdir(this.dataDir);
      const jsonFiles = files.filter(f => f.endsWith('.json'));
      
      const stats = {
        totalFiles: jsonFiles.length,
        hasAllSections: jsonFiles.includes('all-sections.json'),
        individualSections: jsonFiles.filter(f => f !== 'all-sections.json').length
      };
      
      if (stats.hasAllSections) {
        const allSections = await this.loadAllSections();
        stats.totalSections = allSections.length;
        stats.successfulSections = allSections.filter(s => s.success).length;
      }
      
      return stats;
    } catch (error) {
      return {
        totalFiles: 0,
        hasAllSections: false,
        individualSections: 0,
        totalSections: 0,
        successfulSections: 0
      };
    }
  }
}

module.exports = { TestDataLoader };
