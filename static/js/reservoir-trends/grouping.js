/**
 * Grouping logic for reservoir trends
 * Handles reservoir name aliases and aggregation
 */

export class ReservoirGrouping {
    constructor() {
        // Grouping rules - regex patterns to match reservoir variants
        this.rules = [
            {
                label: 'EAGLE FORD variants',
                re: /^EAGLE FORD(?:\b|[-_]\d+)?$/i,
                target: 'EAGLE FORD'
            },
            {
                label: 'WOLFCAMP variants', 
                re: /^WOLFCAMP(?:\b|[-_]\d+)?$/i,
                target: 'WOLFCAMP'
            },
            {
                label: 'SPRABERRY variants',
                re: /^SPRABERRY(?:\b|[-_]\d+)?$/i,
                target: 'SPRABERRY'
            },
            {
                label: 'BONE SPRING variants',
                re: /^BONE SPRING(?:\b|[-_]\d+)?$/i,
                target: 'BONE SPRING'
            },
            {
                label: 'AUSTIN CHALK variants',
                re: /^AUSTIN CHALK(?:\b|[-_]\d+)?$/i,
                target: 'AUSTIN CHALK'
            },
            {
                label: 'BARNETT variants',
                re: /^BARNETT(?:\b|[-_]\d+)?$/i,
                target: 'BARNETT'
            },
            {
                label: 'DELAWARE variants',
                re: /^DELAWARE(?:\b|[-_]\d+)?$/i,
                target: 'DELAWARE'
            },
            {
                label: 'PERMIAN variants',
                re: /^PERMIAN(?:\b|[-_]\d+)?$/i,
                target: 'PERMIAN'
            }
        ];
    }

    /**
     * Normalize reservoir name and find target group
     */
    getTargetName(original) {
        const normalized = original.trim().toUpperCase();
        
        for (const rule of this.rules) {
            if (rule.re.test(normalized)) {
                return rule.target;
            }
        }
        
        return normalized;
    }

    /**
     * Group series data by reservoir names
     * @param {Array} rawSeries - Array of {name, points: [{date, y}]}
     * @returns {Array} Grouped series data
     */
    groupSeries(rawSeries) {
        const buckets = new Map(); // target -> date -> sum
        
        for (const series of rawSeries) {
            const targetName = this.getTargetName(series.name);
            
            if (!buckets.has(targetName)) {
                buckets.set(targetName, new Map());
            }
            
            const byDate = buckets.get(targetName);
            
            for (const point of series.points) {
                const currentSum = byDate.get(point.date) || 0;
                byDate.set(point.date, currentSum + point.y);
            }
        }
        
        // Convert back to series format
        const groupedSeries = [];
        for (const [name, byDate] of buckets.entries()) {
            const points = Array.from(byDate.entries())
                .sort((a, b) => a[0].localeCompare(b[0]))
                .map(([date, y]) => ({ date, y }));
                
            groupedSeries.push({
                name,
                points,
                originalNames: this.getOriginalNamesForTarget(name, rawSeries)
            });
        }
        
        return groupedSeries;
    }

    /**
     * Get original names that map to a target group
     */
    getOriginalNamesForTarget(targetName, rawSeries) {
        return rawSeries
            .filter(series => this.getTargetName(series.name) === targetName)
            .map(series => series.name);
    }

    /**
     * Convert Chart.js dataset format to internal series format
     */
    datasetsToSeries(datasets, labels) {
        return datasets.map(dataset => ({
            name: dataset.label,
            points: labels.map((date, index) => ({
                date,
                y: dataset.data[index] || 0
            }))
        }));
    }

    /**
     * Convert internal series format back to Chart.js dataset format
     */
    seriesToDatasets(series, labels, colors) {
        return series.map((s, index) => ({
            label: s.name,
            data: labels.map(date => {
                const point = s.points.find(p => p.date === date);
                return point ? point.y : 0;
            }),
            borderColor: colors[index % colors.length],
            backgroundColor: colors[index % colors.length] + '20',
            tension: 0.4,
            fill: false,
            pointRadius: 3,
            pointHoverRadius: 6,
            originalNames: s.originalNames || []
        }));
    }

    /**
     * Find singleton series (max cumulative count <= 1 in date range)
     */
    findSingletons(series, startDate, endDate) {
        return series.filter(s => {
            const maxInRange = this.getMaxInRange(s.points, startDate, endDate);
            return maxInRange <= 1;
        }).map(s => s.name);
    }

    /**
     * Get maximum value in a date range
     */
    getMaxInRange(points, startDate, endDate) {
        let max = 0;
        for (const point of points) {
            if (point.date >= startDate && point.date <= endDate) {
                max = Math.max(max, point.y);
            }
        }
        return max;
    }

    /**
     * Get top N series by total count in date range
     */
    getTopNSeries(series, startDate, endDate, n = 10) {
        const seriesWithTotals = series.map(s => ({
            ...s,
            totalInRange: this.getTotalInRange(s.points, startDate, endDate)
        }));
        
        return seriesWithTotals
            .sort((a, b) => b.totalInRange - a.totalInRange)
            .slice(0, n)
            .map(s => s.name);
    }

    /**
     * Get total value in a date range
     */
    getTotalInRange(points, startDate, endDate) {
        let total = 0;
        for (const point of points) {
            if (point.date >= startDate && point.date <= endDate) {
                total += point.y;
            }
        }
        return total;
    }

    /**
     * Update grouping rules (for future configuration)
     */
    updateRules(newRules) {
        this.rules = newRules;
    }

    /**
     * Get current grouping rules
     */
    getRules() {
        return this.rules;
    }
}
