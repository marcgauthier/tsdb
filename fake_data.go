package tsdb

import (
	"fmt"
	"time"
)

// ProbeWithSite identifies one probe and the site it belongs to.
// ProbeID maps to the engine's testID field.
type ProbeWithSite struct {
	ProbeID int64
	SiteID  int64
}

// SiteProbe identifies one synthetic series to generate.
// ProbeID maps to the engine's testID field.
type SiteProbe struct {
	SiteID  int64
	ProbeID int64
}

// GenerateFakeData generates synthetic data for the last `numberOfDays` days at `interval`.
// siteIDs selects which sites to generate for. If empty, all probe siteIDs are used.
// probeIDs carries explicit {probeID, siteID} pairs.
func (e *Engine) GenerateFakeData(siteIDs []int64, probeIDs []ProbeWithSite, numberOfDays int, interval time.Duration) (int, error) {
	if numberOfDays <= 0 {
		return 0, fmt.Errorf("numberOfDays must be > 0")
	}

	endTime := time.Now().Unix()
	startTime := endTime - int64((time.Duration(numberOfDays)*24*time.Hour)/time.Second)
	return e.GenerateFakeDataRange(siteIDs, probeIDs, startTime, endTime, interval)
}

// GenerateFakeDataRange generates synthetic data for [startTime, endTime] at `interval`.
// Timestamps are Unix seconds.
func (e *Engine) GenerateFakeDataRange(siteIDs []int64, probeIDs []ProbeWithSite, startTime, endTime int64, interval time.Duration) (int, error) {
	if len(probeIDs) == 0 {
		return 0, fmt.Errorf("probeIDs list cannot be empty")
	}
	if endTime < startTime {
		return 0, fmt.Errorf("endTime must be >= startTime")
	}

	stepSecs := int64(interval / time.Second)
	if stepSecs <= 0 {
		return 0, fmt.Errorf("interval must be >= 1s")
	}

	allowedSites := make(map[int64]struct{}, len(siteIDs))
	for _, siteID := range siteIDs {
		allowedSites[siteID] = struct{}{}
	}

	type pairKey struct {
		siteID  int64
		probeID int64
	}
	seen := make(map[pairKey]struct{}, len(probeIDs))
	selected := make([]ProbeWithSite, 0, len(probeIDs))
	for _, p := range probeIDs {
		if len(allowedSites) > 0 {
			if _, ok := allowedSites[p.SiteID]; !ok {
				continue
			}
		}

		k := pairKey{siteID: p.SiteID, probeID: p.ProbeID}
		if _, exists := seen[k]; exists {
			continue
		}
		seen[k] = struct{}{}
		selected = append(selected, p)
	}

	if len(selected) == 0 {
		return 0, fmt.Errorf("no probeIDs matched provided siteIDs")
	}

	total := 0
	for ts := startTime; ts <= endTime; ts += stepSecs {
		for _, p := range selected {
			v := syntheticValue(p.SiteID, p.ProbeID, ts, startTime, stepSecs)
			e.Add(ts, p.SiteID, p.ProbeID, v)
			total++
		}
	}

	return total, nil
}

// GenerateFakeDataForSeries is a compatibility helper for series-only inputs.
func (e *Engine) GenerateFakeDataForSeries(series []SiteProbe, days int, interval time.Duration) (int, error) {
	probeIDs := make([]ProbeWithSite, 0, len(series))
	for _, s := range series {
		probeIDs = append(probeIDs, ProbeWithSite{
			ProbeID: s.ProbeID,
			SiteID:  s.SiteID,
		})
	}
	return e.GenerateFakeData(nil, probeIDs, days, interval)
}

// GenerateFakeDataRangeForSeries is a compatibility helper for series-only inputs.
func (e *Engine) GenerateFakeDataRangeForSeries(series []SiteProbe, startTime, endTime int64, interval time.Duration) (int, error) {
	probeIDs := make([]ProbeWithSite, 0, len(series))
	for _, s := range series {
		probeIDs = append(probeIDs, ProbeWithSite{
			ProbeID: s.ProbeID,
			SiteID:  s.SiteID,
		})
	}
	return e.GenerateFakeDataRange(nil, probeIDs, startTime, endTime, interval)
}

// syntheticValue is deterministic so repeated runs are reproducible.
func syntheticValue(siteID, probeID, ts, startTime, stepSecs int64) int64 {
	base := int64(500 + (siteID%50)*7 + (probeID%30)*5)
	slot := (ts - startTime) / stepSecs

	// Simple triangular daily cycle in [0..60].
	hour := (ts % 86400) / 3600
	cycle := hour
	if cycle > 11 {
		cycle = 23 - cycle
	}
	daily := cycle * 5

	// Slow upward trend and deterministic jitter.
	trend := slot / 120
	jitter := int64((uint64(siteID*73856093)^uint64(probeID*19349663)^uint64(ts*83492791))%13) - 6

	value := base + daily + trend + jitter
	if value < 0 {
		return 0
	}
	return value
}
