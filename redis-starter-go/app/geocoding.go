package main

import (
	"math"
)

const (
	MIN_LATITUDE  = -85.05112878
	MAX_LATITUDE  = 85.05112878
	MIN_LONGITUDE = -180.0
	MAX_LONGITUDE = 180.0

	LATITUDE_RANGE  = MAX_LATITUDE - MIN_LATITUDE
	LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE

	EARTH_RADIUS = 6372797.560856
)

type Coordinates struct {
	Latitude  float64
	Longitude float64
}

type ScoreRange struct {
	Min uint64
	Max uint64
}

func spreadInt32ToInt64(v uint32) uint64 {
	result := uint64(v)
	result = (result | (result << 16)) & 0x0000FFFF0000FFFF
	result = (result | (result << 8)) & 0x00FF00FF00FF00FF
	result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F
	result = (result | (result << 2)) & 0x3333333333333333
	result = (result | (result << 1)) & 0x5555555555555555
	return result
}

func compactInt64ToInt32(v uint64) uint32 {
	result := v & 0x5555555555555555
	result = (result | (result >> 1)) & 0x3333333333333333
	result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F
	result = (result | (result >> 4)) & 0x00FF00FF00FF00FF
	result = (result | (result >> 8)) & 0x0000FFFF0000FFFF
	result = (result | (result >> 16)) & 0x00000000FFFFFFFF
	return uint32(result)
}

func interleave(x, y uint32) uint64 {
	xSpread := spreadInt32ToInt64(x)
	ySpread := spreadInt32ToInt64(y)
	yShifted := ySpread << 1
	return xSpread | yShifted
}

func convertGridNumbersToCoordinates(gridLatitudeNumber, gridLongitudeNumber uint32) Coordinates {
	// Calculate the grid boundaries
	gridLatitudeMin := MIN_LATITUDE + LATITUDE_RANGE*(float64(gridLatitudeNumber)/math.Pow(2, 26))
	gridLatitudeMax := MIN_LATITUDE + LATITUDE_RANGE*(float64(gridLatitudeNumber+1)/math.Pow(2, 26))
	gridLongitudeMin := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(gridLongitudeNumber)/math.Pow(2, 26))
	gridLongitudeMax := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(gridLongitudeNumber+1)/math.Pow(2, 26))

	// Calculate the center point of the grid cell
	latitude := (gridLatitudeMin + gridLatitudeMax) / 2
	longitude := (gridLongitudeMin + gridLongitudeMax) / 2

	return Coordinates{Latitude: latitude, Longitude: longitude}
}

func Encode(latitude, longitude float64) uint64 {
	// Normalize to the range 0-2^26
	normalizedLatitude := math.Pow(2, 26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE
	normalizedLongitude := math.Pow(2, 26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE

	// Truncate to integers
	latInt := uint32(normalizedLatitude)
	lonInt := uint32(normalizedLongitude)

	return interleave(latInt, lonInt)
}

func Decode(geoCode uint64) Coordinates {
	// Align bits of both latitude and longitude to take even-numbered position
	y := geoCode >> 1
	x := geoCode

	// Compact bits back to 32-bit ints
	gridLatitudeNumber := compactInt64ToInt32(x)
	gridLongitudeNumber := compactInt64ToInt32(y)

	return convertGridNumbersToCoordinates(gridLatitudeNumber, gridLongitudeNumber)
}

func GeoDistance(a, b Coordinates) float64 {
	lon1r := a.Longitude * (math.Pi / 180)
	lon2r := b.Longitude * (math.Pi / 180)

	v := math.Sin((lon2r - lon1r) / 2)

	lat1r := a.Latitude * (math.Pi / 180)
	lat2r := b.Latitude * (math.Pi / 180)

	u := math.Sin((lat2r - lat1r) / 2)

	d := u*u + math.Cos(lat1r)*math.Cos(lat2r)*v*v

	return 2.0 * EARTH_RADIUS * math.Asin(math.Sqrt(d))
}

func Get9BoxRanges(centerLat, centerLon, radiusMeters float64) []ScoreRange {
	depth := 26

	latRad := centerLat * math.Pi / 180.0
	cosLat := math.Cos(latRad)

	if cosLat < 0.01 {
		cosLat = 0.01 // Preventing division by zero
	}

	for depth > 1 {
		degWidth := 360.0 / float64(uint32(1)<<depth)
		metersWidth := degWidth * (math.Pi / 180.0) * EARTH_RADIUS * cosLat

		degHeight := LATITUDE_RANGE / float64(uint32(1)<<depth)
		metersHeight := degHeight * (math.Pi / 180.0) * EARTH_RADIUS

		if metersWidth >= radiusMeters && metersHeight >= radiusMeters {
			break
		}
		depth--
	}

	normalizedLat := math.Pow(2, 26) * (centerLat - MIN_LATITUDE) / LATITUDE_RANGE
	normalizedLon := math.Pow(2, 26) * (centerLon - MIN_LONGITUDE) / LONGITUDE_RANGE

	shift := 26 - depth
	gridX := uint32(normalizedLon) >> shift
	gridY := uint32(normalizedLat) >> shift

	var ranges []ScoreRange
	maxGrid := int(uint32(1<<depth) - 1)

	for dx := -1; dx <= 1; dx++ {
		for dy := -1; dy <= 1; dy++ {
			nx := int(gridX) + dx
			ny := int(gridY) + dy

			if nx < 0 || nx > maxGrid || ny < 0 || ny > maxGrid {
				continue
			}

			boxScore := interleave(uint32(nx), uint32(ny))

			minScore := boxScore << (2 * shift)
			maxScore := minScore | ((uint64(1) << (2 * shift)) - 1)

			ranges = append(ranges, ScoreRange{Min: minScore, Max: maxScore})
		}
	}

	return ranges
}
