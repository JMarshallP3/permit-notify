// RESERVOIR MANAGER MEMORY LEAK DETECTION SCRIPT
// Run this in your browser console (F12) on the permit tracker page

window.MemoryLeakDetector = {
    baseline: null,
    cycles: [],
    
    // Record baseline metrics before any interaction
    recordBaseline() {
        console.log('🔍 Recording baseline metrics...');
        
        this.baseline = {
            timestamp: Date.now(),
            domNodes: document.getElementsByTagName('*').length,
            
            // Modal instances
            fixedModals: document.querySelectorAll('.fixed, [style*="position: fixed"]').length,
            reservoirModals: document.querySelectorAll('[class*="reservoir"], [id*="reservoir"]').length,
            
            // Event listeners (if available)
            windowListeners: this.getListenerCount(window),
            documentListeners: this.getListenerCount(document),
            
            // Memory info (if available)
            memoryInfo: performance.memory ? {
                used: performance.memory.usedJSHeapSize,
                total: performance.memory.totalJSHeapSize,
                limit: performance.memory.jsHeapSizeLimit
            } : null,
            
            // Dashboard instance check
            dashboardExists: !!window.dashboard,
            dashboardType: window.dashboard ? window.dashboard.constructor.name : null
        };
        
        console.log('📊 Baseline recorded:', this.baseline);
        return this.baseline;
    },
    
    // Record metrics after each open/close cycle
    recordCycle(cycleNum, phase) {
        const current = {
            cycle: cycleNum,
            phase: phase, // 'open' or 'close'
            timestamp: Date.now(),
            domNodes: document.getElementsByTagName('*').length,
            
            // Modal instances
            fixedModals: document.querySelectorAll('.fixed, [style*="position: fixed"]').length,
            reservoirModals: document.querySelectorAll('[class*="reservoir"], [id*="reservoir"]').length,
            
            // Specific reservoir manager elements
            reservoirTabs: document.querySelectorAll('.reservoir-tab').length,
            reservoirContent: document.querySelectorAll('[id*="reservoirTabContent"]').length,
            
            // Event listeners
            windowListeners: this.getListenerCount(window),
            documentListeners: this.getListenerCount(document),
            
            // Memory info
            memoryInfo: performance.memory ? {
                used: performance.memory.usedJSHeapSize,
                total: performance.memory.totalJSHeapSize,
                limit: performance.memory.jsHeapSizeLimit
            } : null,
            
            // Calculate deltas from baseline
            deltas: this.baseline ? {
                domNodes: document.getElementsByTagName('*').length - this.baseline.domNodes,
                fixedModals: document.querySelectorAll('.fixed, [style*="position: fixed"]').length - this.baseline.fixedModals,
                memoryUsed: performance.memory ? (performance.memory.usedJSHeapSize - this.baseline.memoryInfo.used) : null
            } : null
        };
        
        this.cycles.push(current);
        console.log(`📈 Cycle ${cycleNum} (${phase}):`, current);
        
        // Alert if major leak detected
        if (current.deltas && current.deltas.domNodes > 1000) {
            console.warn(`🚨 MAJOR DOM LEAK: +${current.deltas.domNodes} nodes in cycle ${cycleNum}`);
        }
        
        return current;
    },
    
    // Get event listener count (best effort)
    getListenerCount(element) {
        try {
            const listeners = getEventListeners ? getEventListeners(element) : {};
            const counts = {};
            let total = 0;
            
            for (const [type, list] of Object.entries(listeners)) {
                counts[type] = Array.isArray(list) ? list.length : 1;
                total += counts[type];
            }
            
            return { total, byType: counts };
        } catch (e) {
            return { total: 'unknown', byType: {}, error: e.message };
        }
    },
    
    // Automated test runner
    async runAutomatedTest() {
        console.log('🚀 Starting automated reservoir manager memory leak test...');
        
        // Record baseline
        this.recordBaseline();
        
        // Wait a bit for stability
        await this.sleep(1000);
        
        // Run 5 open/close cycles
        for (let i = 1; i <= 5; i++) {
            console.log(`\n🔄 Starting cycle ${i}/5...`);
            
            // Open reservoir manager
            try {
                if (window.dashboard && window.dashboard.openReservoirManager) {
                    window.dashboard.openReservoirManager();
                    await this.sleep(500); // Wait for modal to render
                    this.recordCycle(i, 'open');
                } else {
                    console.error('❌ window.dashboard.openReservoirManager not found');
                    break;
                }
            } catch (e) {
                console.error(`❌ Error opening reservoir manager in cycle ${i}:`, e);
                break;
            }
            
            // Close modal
            try {
                const modal = document.querySelector('.fixed');
                if (modal) {
                    modal.remove();
                    await this.sleep(500); // Wait for cleanup
                    this.recordCycle(i, 'close');
                } else {
                    console.warn(`⚠️ No modal found to close in cycle ${i}`);
                }
            } catch (e) {
                console.error(`❌ Error closing modal in cycle ${i}:`, e);
            }
            
            // Wait between cycles
            await this.sleep(1000);
        }
        
        // Generate report
        this.generateReport();
    },
    
    // Generate detailed report
    generateReport() {
        console.log('\n📋 MEMORY LEAK DETECTION REPORT');
        console.log('================================');
        
        if (!this.baseline || this.cycles.length === 0) {
            console.log('❌ No data collected');
            return;
        }
        
        const lastCycle = this.cycles[this.cycles.length - 1];
        const finalDeltas = lastCycle.deltas;
        
        console.log('\n🎯 FINDINGS:');
        console.log(`- Modal instances after 5 cycles: ${lastCycle.fixedModals} (expected: 0)`);
        console.log(`- DOM nodes baseline vs after close: ${this.baseline.domNodes} → ${lastCycle.domNodes} (delta: +${finalDeltas.domNodes})`);
        console.log(`- Reservoir-specific elements: ${lastCycle.reservoirModals} modals, ${lastCycle.reservoirTabs} tabs, ${lastCycle.reservoirContent} content`);
        
        if (finalDeltas.memoryUsed !== null) {
            console.log(`- Memory usage delta: +${(finalDeltas.memoryUsed / 1024 / 1024).toFixed(2)} MB`);
        }
        
        console.log('\n📊 Event Listeners:');
        console.log(`- Window listeners: ${this.baseline.windowListeners.total} → ${lastCycle.windowListeners.total}`);
        console.log(`- Document listeners: ${this.baseline.documentListeners.total} → ${lastCycle.documentListeners.total}`);
        
        console.log('\n🔍 LEAK ANALYSIS:');
        
        // Check for persistent modals
        if (lastCycle.fixedModals > 0) {
            console.log(`🚨 LEAK: ${lastCycle.fixedModals} modal(s) still in DOM after close`);
        }
        
        // Check for DOM node accumulation
        if (finalDeltas.domNodes > 100) {
            console.log(`🚨 LEAK: ${finalDeltas.domNodes} excess DOM nodes accumulated`);
        }
        
        // Check for listener accumulation
        const listenerGrowth = lastCycle.windowListeners.total - this.baseline.windowListeners.total;
        if (listenerGrowth > 5) {
            console.log(`🚨 LEAK: ${listenerGrowth} excess event listeners on window`);
        }
        
        console.log('\n📈 Cycle-by-cycle data:');
        console.table(this.cycles.map(c => ({
            Cycle: c.cycle,
            Phase: c.phase,
            'DOM Nodes': c.domNodes,
            'DOM Delta': c.deltas ? c.deltas.domNodes : 0,
            'Fixed Modals': c.fixedModals,
            'Memory MB': c.memoryInfo ? (c.memoryInfo.used / 1024 / 1024).toFixed(1) : 'N/A'
        })));
        
        // Store results for manual inspection
        window.leakTestResults = {
            baseline: this.baseline,
            cycles: this.cycles,
            finalDeltas: finalDeltas
        };
        
        console.log('\n💾 Results stored in window.leakTestResults for further analysis');
    },
    
    // Helper function for delays
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    },
    
    // Manual inspection helpers
    inspectCurrentModals() {
        const modals = document.querySelectorAll('.fixed, [style*="position: fixed"]');
        console.log(`Found ${modals.length} modal(s):`);
        modals.forEach((modal, i) => {
            console.log(`Modal ${i + 1}:`, {
                element: modal,
                classes: modal.className,
                id: modal.id,
                innerHTML: modal.innerHTML.substring(0, 200) + '...'
            });
        });
        return modals;
    },
    
    // Check for detached elements (requires manual heap snapshot)
    suggestHeapAnalysis() {
        console.log('\n🔬 HEAP ANALYSIS INSTRUCTIONS:');
        console.log('1. Open DevTools → Memory tab');
        console.log('2. Take heap snapshot');
        console.log('3. Search for "Detached"');
        console.log('4. Look for HTMLDivElement with class="fixed" or reservoir-related classes');
        console.log('5. Click on detached element → Retainers tab');
        console.log('6. Follow retainer chain to find what\'s holding references');
        console.log('\nCommon retainers to look for:');
        console.log('- Event listeners (onclick, addEventListener)');
        console.log('- Closure variables in dashboard functions');
        console.log('- Arrays or objects in window.dashboard');
    }
};

// Auto-run if dashboard is available
if (typeof window !== 'undefined' && window.dashboard) {
    console.log('🎯 Reservoir Manager Memory Leak Detector loaded!');
    console.log('Run: MemoryLeakDetector.runAutomatedTest()');
    console.log('Or manually: MemoryLeakDetector.recordBaseline() then open/close modals');
} else {
    console.log('⚠️ window.dashboard not found. Load this script on the permit tracker page.');
}
