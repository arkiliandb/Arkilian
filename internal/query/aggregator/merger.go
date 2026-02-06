package aggregator

// AggregateMerger merges partial aggregates from multiple partitions
// into a single final result.
type AggregateMerger struct{}

// NewAggregateMerger creates a new aggregate merger.
func NewAggregateMerger() *AggregateMerger {
	return &AggregateMerger{}
}

// MergePartials combines multiple PartialAggregate values (one per partition)
// into a single merged PartialAggregate. The merge rules are:
//   - COUNT: sum of counts
//   - SUM:   sum of sums
//   - MIN:   minimum of mins
//   - MAX:   maximum of maxes
//   - AVG:   weighted average using (sum of sums) / (sum of counts)
func MergePartials(partials []*PartialAggregate) *PartialAggregate {
	if len(partials) == 0 {
		return &PartialAggregate{}
	}

	aggType := partials[0].Type
	merged := &PartialAggregate{Type: aggType}

	for _, p := range partials {
		if !p.IsSet {
			continue
		}

		switch aggType {
		case AggCount:
			merged.Count += p.Count
			merged.IsSet = true

		case AggSum:
			merged.Sum += p.Sum
			merged.Count += p.Count
			merged.IsSet = true

		case AggMin:
			if !merged.IsSet || compareAggValues(p.Min, merged.Min) < 0 {
				merged.Min = p.Min
			}
			merged.Count += p.Count
			merged.IsSet = true

		case AggMax:
			if !merged.IsSet || compareAggValues(p.Max, merged.Max) > 0 {
				merged.Max = p.Max
			}
			merged.Count += p.Count
			merged.IsSet = true

		case AggAvg:
			merged.Sum += p.Sum
			merged.Count += p.Count
			merged.IsSet = true
		}
	}

	return merged
}

// MergePartialSets merges multiple PartialAggregateSets (one per partition)
// into a single set of final aggregate values.
func MergePartialSets(sets []*PartialAggregateSet) []interface{} {
	if len(sets) == 0 {
		return nil
	}

	numAggs := len(sets[0].Aggregates)
	results := make([]interface{}, numAggs)

	for i := 0; i < numAggs; i++ {
		// Collect the i-th partial from each set
		partials := make([]*PartialAggregate, 0, len(sets))
		for _, set := range sets {
			if i < len(set.Aggregates) {
				partials = append(partials, set.Aggregates[i])
			}
		}
		merged := MergePartials(partials)
		results[i] = merged.Result()
	}

	return results
}
