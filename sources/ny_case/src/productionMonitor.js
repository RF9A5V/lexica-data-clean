#!/usr/bin/env node

/**
 * Production Monitoring Dashboard for Keyword Extraction System
 * 
 * Provides real-time monitoring of the keyword extraction process,
 * system health checks, and production deployment recommendations.
 * 
 * Usage: node productionMonitor.js [refresh_interval_seconds]
 */

import { Client } from 'pg';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const CONFIG = {
  dbUrl: 'postgresql://localhost/ny_court_of_appeals',
  refreshInterval: parseInt(process.argv[2]) || 30, // seconds
  maxRecommendedBatch: 200,
  optimalConcurrency: 4
};

class ProductionMonitor {
  constructor() {
    this.startTime = Date.now();
    this.lastStats = null;
  }

  async getSystemStats(pg) {
    // Get overall database statistics
    const overallStats = await pg.query(`
      SELECT 
        COUNT(*) as total_binding_opinions,
        COUNT(CASE WHEN ok.opinion_id IS NOT NULL THEN 1 END) as opinions_with_keywords,
        COUNT(CASE WHEN ok.opinion_id IS NULL THEN 1 END) as opinions_without_keywords
      FROM opinions o
      LEFT JOIN opinion_keywords ok ON o.id = ok.opinion_id
      WHERE o.binding_type IN ('015unanimous', '010combined', '020lead')
    `);

    // Get keyword statistics
    const keywordStats = await pg.query(`
      SELECT 
        COUNT(DISTINCT ok.opinion_id) as opinions_with_keywords,
        COUNT(*) as total_keyword_assignments,
        COUNT(DISTINCT k.keyword_text) as unique_keywords,
        AVG(ok.relevance_score) as avg_relevance,
        MIN(ok.created_at) as first_extraction,
        MAX(ok.created_at) as last_extraction
      FROM opinion_keywords ok
      JOIN keywords k ON ok.keyword_id = k.id
      JOIN opinions o ON ok.opinion_id = o.id
      WHERE o.binding_type IN ('015unanimous', '010combined', '020lead')
    `);

    // Get recent processing activity (last 24 hours)
    const recentActivity = await pg.query(`
      SELECT 
        COUNT(DISTINCT ok.opinion_id) as opinions_processed_24h,
        COUNT(*) as keywords_extracted_24h,
        AVG(ok.relevance_score) as avg_relevance_24h
      FROM opinion_keywords ok
      JOIN opinions o ON ok.opinion_id = o.id
      WHERE o.binding_type IN ('015unanimous', '010combined', '020lead')
        AND ok.created_at >= NOW() - INTERVAL '24 hours'
    `);

    // Get category distribution
    const categoryStats = await pg.query(`
      SELECT 
        ok.category,
        COUNT(*) as count,
        AVG(ok.relevance_score) as avg_relevance
      FROM opinion_keywords ok
      JOIN opinions o ON ok.opinion_id = o.id
      WHERE o.binding_type IN ('015unanimous', '010combined', '020lead')
      GROUP BY ok.category
      ORDER BY count DESC
    `);

    // Get quality distribution
    const qualityStats = await pg.query(`
      SELECT 
        CASE 
          WHEN ok.relevance_score >= 0.9 THEN 'Excellent (0.9-1.0)'
          WHEN ok.relevance_score >= 0.8 THEN 'High (0.8-0.9)'
          WHEN ok.relevance_score >= 0.7 THEN 'Good (0.7-0.8)'
          WHEN ok.relevance_score >= 0.6 THEN 'Fair (0.6-0.7)'
          ELSE 'Low (0.5-0.6)'
        END as quality_tier,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
      FROM opinion_keywords ok
      JOIN opinions o ON ok.opinion_id = o.id
      WHERE o.binding_type IN ('015unanimous', '010combined', '020lead')
      GROUP BY quality_tier
      ORDER BY MIN(ok.relevance_score) DESC
    `);

    return {
      overall: overallStats.rows[0],
      keywords: keywordStats.rows[0],
      recent: recentActivity.rows[0],
      categories: categoryStats.rows,
      quality: qualityStats.rows
    };
  }

  calculateProcessingRate(stats) {
    if (!this.lastStats) {
      return null;
    }

    const timeDiff = (Date.now() - this.lastUpdateTime) / 1000; // seconds
    const opinionsDiff = parseInt(stats.keywords.opinions_with_keywords) - parseInt(this.lastStats.keywords.opinions_with_keywords);
    const keywordsDiff = parseInt(stats.keywords.total_keyword_assignments) - parseInt(this.lastStats.keywords.total_keyword_assignments);

    return {
      opinionsPerSecond: opinionsDiff / timeDiff,
      keywordsPerSecond: keywordsDiff / timeDiff,
      timeDiff: timeDiff
    };
  }

  getRecommendations(stats) {
    const remaining = parseInt(stats.overall.opinions_without_keywords);
    const processed = parseInt(stats.overall.opinions_with_keywords);
    const total = parseInt(stats.overall.total_binding_opinions);
    const completionRate = (processed / total) * 100;

    const recommendations = [];

    // Processing recommendations
    if (remaining === 0) {
      recommendations.push({
        type: 'success',
        message: '🎉 All opinions have been processed! System is up to date.',
        action: 'Consider running periodic updates for new opinions.'
      });
    } else if (remaining < 50) {
      recommendations.push({
        type: 'info',
        message: `🎯 Only ${remaining} opinions remaining - ready for final processing.`,
        action: `node batchKeywordExtraction.js ${remaining} 3`
      });
    } else if (remaining < 200) {
      recommendations.push({
        type: 'info',
        message: `📈 ${remaining} opinions remaining - good for medium batch processing.`,
        action: `node batchKeywordExtraction.js ${Math.min(remaining, 100)} 4`
      });
    } else if (remaining < 1000) {
      recommendations.push({
        type: 'warning',
        message: `⚡ ${remaining} opinions remaining - consider chunked processing.`,
        action: `Process in batches: node batchKeywordExtraction.js 200 4`
      });
    } else {
      recommendations.push({
        type: 'warning',
        message: `🚀 ${remaining} opinions remaining - large-scale processing needed.`,
        action: 'Process in multiple sessions with 200-500 opinion batches.'
      });
    }

    // Quality recommendations
    const avgRelevance = parseFloat(stats.keywords.avg_relevance || 0);
    if (avgRelevance < 0.8) {
      recommendations.push({
        type: 'warning',
        message: `📊 Average relevance score (${avgRelevance.toFixed(3)}) is below optimal (0.8+).`,
        action: 'Review keyword extraction prompt and quality filters.'
      });
    } else if (avgRelevance >= 0.85) {
      recommendations.push({
        type: 'success',
        message: `✅ Excellent keyword quality (${avgRelevance.toFixed(3)} average relevance).`,
        action: 'Current quality settings are optimal.'
      });
    }

    // Recent activity recommendations
    const recent24h = parseInt(stats.recent.opinions_processed_24h || 0);
    if (recent24h === 0 && remaining > 0) {
      recommendations.push({
        type: 'info',
        message: '💤 No processing activity in the last 24 hours.',
        action: 'Consider resuming keyword extraction processing.'
      });
    } else if (recent24h > 100) {
      recommendations.push({
        type: 'success',
        message: `🔥 High processing activity: ${recent24h} opinions processed in 24h.`,
        action: 'Monitor system resources and API usage.'
      });
    }

    return recommendations;
  }

  formatStats(stats, rate) {
    const remaining = parseInt(stats.overall.opinions_without_keywords);
    const processed = parseInt(stats.overall.opinions_with_keywords);
    const total = parseInt(stats.overall.total_binding_opinions);
    const completionRate = (processed / total) * 100;

    console.clear();
    console.log('🔍 KEYWORD EXTRACTION PRODUCTION MONITOR');
    console.log('=' .repeat(80));
    console.log(`📅 Monitoring since: ${new Date(this.startTime).toLocaleString()}`);
    console.log(`🔄 Last updated: ${new Date().toLocaleString()}`);
    console.log('=' .repeat(80));

    // Overall Progress
    console.log('\n📊 OVERALL PROGRESS');
    console.log('-'.repeat(40));
    console.log(`Total Binding Opinions: ${total.toLocaleString()}`);
    console.log(`✅ Processed: ${processed.toLocaleString()} (${completionRate.toFixed(1)}%)`);
    console.log(`⏳ Remaining: ${remaining.toLocaleString()} (${(100 - completionRate).toFixed(1)}%)`);
    
    // Progress bar
    const barLength = 50;
    const filledLength = Math.round((completionRate / 100) * barLength);
    const bar = '█'.repeat(filledLength) + '░'.repeat(barLength - filledLength);
    console.log(`Progress: |${bar}| ${completionRate.toFixed(1)}%`);

    // Keyword Statistics
    console.log('\n🔑 KEYWORD STATISTICS');
    console.log('-'.repeat(40));
    console.log(`Total Keywords: ${parseInt(stats.keywords.total_keyword_assignments || 0).toLocaleString()}`);
    console.log(`Unique Keywords: ${parseInt(stats.keywords.unique_keywords || 0).toLocaleString()}`);
    console.log(`Average Relevance: ${parseFloat(stats.keywords.avg_relevance || 0).toFixed(3)}`);
    console.log(`Avg Keywords/Opinion: ${(parseInt(stats.keywords.total_keyword_assignments || 0) / Math.max(processed, 1)).toFixed(1)}`);

    // Recent Activity
    console.log('\n⚡ RECENT ACTIVITY (24 Hours)');
    console.log('-'.repeat(40));
    const recent24h = parseInt(stats.recent.opinions_processed_24h || 0);
    const recentKeywords = parseInt(stats.recent.keywords_extracted_24h || 0);
    console.log(`Opinions Processed: ${recent24h.toLocaleString()}`);
    console.log(`Keywords Extracted: ${recentKeywords.toLocaleString()}`);
    if (recent24h > 0) {
      console.log(`Recent Quality: ${parseFloat(stats.recent.avg_relevance_24h || 0).toFixed(3)}`);
    }

    // Processing Rate
    if (rate && rate.timeDiff > 10) {
      console.log('\n📈 PROCESSING RATE');
      console.log('-'.repeat(40));
      console.log(`Opinions/Second: ${rate.opinionsPerSecond.toFixed(3)}`);
      console.log(`Keywords/Second: ${rate.keywordsPerSecond.toFixed(1)}`);
      
      if (rate.opinionsPerSecond > 0) {
        const etaSeconds = remaining / rate.opinionsPerSecond;
        const etaHours = etaSeconds / 3600;
        console.log(`ETA to Completion: ${etaHours.toFixed(1)} hours`);
      }
    }

    // Category Distribution
    console.log('\n📂 CATEGORY DISTRIBUTION');
    console.log('-'.repeat(40));
    stats.categories.forEach(cat => {
      const percentage = (parseInt(cat.count) / parseInt(stats.keywords.total_keyword_assignments) * 100).toFixed(1);
      console.log(`${cat.category}: ${parseInt(cat.count).toLocaleString()} (${percentage}%) - Avg: ${parseFloat(cat.avg_relevance).toFixed(3)}`);
    });

    // Quality Distribution
    console.log('\n⭐ QUALITY DISTRIBUTION');
    console.log('-'.repeat(40));
    stats.quality.forEach(qual => {
      console.log(`${qual.quality_tier}: ${parseInt(qual.count).toLocaleString()} (${qual.percentage}%)`);
    });

    // Recommendations
    const recommendations = this.getRecommendations(stats);
    console.log('\n💡 RECOMMENDATIONS');
    console.log('-'.repeat(40));
    recommendations.forEach((rec, index) => {
      const icon = rec.type === 'success' ? '✅' : rec.type === 'warning' ? '⚠️' : 'ℹ️';
      console.log(`${index + 1}. ${icon} ${rec.message}`);
      console.log(`   Action: ${rec.action}`);
    });

    console.log('\n' + '='.repeat(80));
    console.log(`🔄 Refreshing every ${CONFIG.refreshInterval} seconds... (Press Ctrl+C to exit)`);
  }

  async start() {
    console.log('🚀 Starting Production Monitor...');
    console.log(`📊 Monitoring keyword extraction system every ${CONFIG.refreshInterval} seconds`);
    
    const pg = new Client({ connectionString: CONFIG.dbUrl });
    
    try {
      await pg.connect();
      console.log('✅ Connected to database');
      
      // Initial display
      await this.updateDisplay(pg);
      
      // Set up periodic updates
      setInterval(async () => {
        try {
          await this.updateDisplay(pg);
        } catch (error) {
          console.error('❌ Error updating display:', error.message);
        }
      }, CONFIG.refreshInterval * 1000);
      
    } catch (error) {
      console.error('❌ Failed to connect to database:', error.message);
      process.exit(1);
    }
  }

  async updateDisplay(pg) {
    const stats = await this.getSystemStats(pg);
    const rate = this.calculateProcessingRate(stats);
    
    this.formatStats(stats, rate);
    
    this.lastStats = stats;
    this.lastUpdateTime = Date.now();
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n\n👋 Production monitor stopped.');
  process.exit(0);
});

// Start the monitor
const monitor = new ProductionMonitor();
monitor.start().catch(console.error);
