# Reservoir Trends Widget

Enhanced reservoir trends analysis with advanced UX features including grouping, persistence, and performance optimizations.

## Features

### Quick Controls
- **Hide All / Show All**: Quickly toggle all series visibility
- **Hide Singletons**: Hide reservoirs with max cumulative count ≤ 1 in current range
- **Show Top N**: Display only the most active reservoirs (configurable N=5,10,15,20)

### Reservoir Grouping
- **Name Aliases**: Groups reservoir variants (e.g., EAGLE FORD, EAGLE FORD-1, EAGLE FORD-2 → EAGLE FORD)
- **Configurable Rules**: Regex-based grouping rules for different basins
- **Aggregation**: Sums counts for grouped reservoirs by date

### State Persistence
- **Per User + Org**: State saved separately for each user and organization
- **Persisted Settings**:
  - Hidden/visible series set
  - Grouping on/off + mapping version
  - Time range selector value
  - View mode (Cumulative vs Daily)
- **Storage**: localStorage with optional API fallback

### Enhanced Hover Behavior
- **Single Series**: Shows only hovered series name and count (not all selected)
- **Pixel Threshold**: Only shows tooltip when cursor is within sensible distance of line
- **No Spam**: Moving off lines shows nothing

### Performance Optimizations
- **Debounced Updates**: Expensive recalculations debounced (200ms)
- **View Models**: Raw → Grouped → Visible data pipeline
- **Smooth Interactions**: Toggling visibility, grouping, and hover optimized for 100+ series

## File Structure

```
reservoir-trends/
├── modal.js          # Main modal component
├── chart.js          # Enhanced Chart.js implementation
├── state.js          # State management and persistence
├── grouping.js       # Reservoir name grouping logic
└── README.md         # This file
```

## Usage

```javascript
// Import and use the enhanced modal
import { ReservoirTrendsModal } from './reservoir-trends/modal.js';

const modal = new ReservoirTrendsModal();
await modal.open(specificReservoir);
```

## Grouping Rules

Default grouping patterns (configurable):

```javascript
const rules = [
    { label: 'EAGLE FORD variants', re: /^EAGLE FORD(?:\b|[-_]\d+)?$/i, target: 'EAGLE FORD' },
    { label: 'WOLFCAMP variants', re: /^WOLFCAMP(?:\b|[-_]\d+)?$/i, target: 'WOLFCAMP' },
    { label: 'SPRABERRY variants', re: /^SPRABERRY(?:\b|[-_]\d+)?$/i, target: 'SPRABERRY' },
    // ... more rules
];
```

## State Keys

localStorage keys follow the pattern:
```
reservoirTrends:{orgId}:{userId}
```

Example: `reservoirTrends:default_org:user123`

## API Endpoints

- `GET /api/user_prefs?key={key}` - Retrieve user preferences
- `PATCH /api/user_prefs` - Update user preferences

## Chart.js Integration

Uses Chart.js v3+ with enhanced configuration:
- Custom tooltip formatter for single-series display
- Optimized hover behavior with pixel thresholds
- Legend click handlers for series visibility
- Responsive design with proper aspect ratio

## Browser Support

- Modern browsers with ES6 modules support
- localStorage required for state persistence
- Chart.js v3+ required

## Performance Notes

- Debounced state saves prevent excessive localStorage writes
- View model pattern ensures efficient data transformations
- Chart updates use 'none' mode for smooth animations
- Memory management with proper chart destruction
