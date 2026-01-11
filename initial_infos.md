Nextbike API endpoints and comparison (Karlsruhe)
Background
The German city of Karlsruhe is served by KVV.nextbike, a bike‑sharing network operated by the company Nextbike. City planners and researchers may want to analyse station and bike usage for urban planning and infrastructure decisions. Nextbike exposes several unauthenticated web APIs that return live data on bikes, stations and service zones. I explored the publicly available endpoints and compared them to identify which API delivers the richest data. The endpoints were tested in January 2026 from Karlsruhe’s domain fg. The findings below include documented response structures and sample parameters, with citations from primary sources.

Overview of Nextbike APIs
API/Endpoint	Major content	Key parameters (examples)	Data richness
Live availability feed (https://maps.nextbike.net/maps/nextbike‑live.{format})	Returns a nested structure of countries, cities, places and bikes. Each bike entry contains the bike number, type, lock types, active/state flags, electric_lock indicator, board‑computer ID and battery information. For example, in Karlsruhe a place record lists bikes with fields like number, bike_type, lock_types, active, state, electric_lock, boardcomputer, pedelec_battery and battery_pack percentage
.	scope (always live), format (json, xml or flatjson), city, domains, countries, place, station, lat/lng with distance, list_cities (include city list), bikes (omit per‑bike objects). Multiple values are comma‑separated.	Richest: contains per‑bike details, including board‑computer ID and battery percentage
. Supports filtering by domain or city.
Official feed (https://maps.nextbike.net/maps/nextbike‑official.{format})	Same structure as the live feed but includes only official stations. Bikes have the same detailed fields as above
.	Same as live feed.	High – detailed per‑bike data but fewer bikes because free‑floating bikes and flex zones are excluded.
Flat JSON variant (…/nextbike‑live.flatjson)	Flattens the live feed into separate arrays for countries, cities and places. Per‑bike objects are omitted; the bike_numbers field is a comma‑separated string and bike_types is a JSON string
.	Same parameters as the live feed.	Moderate – easier to parse but discards board‑computer and battery details.
General Bikeshare Feed Specification (GBFS) (https://gbfs.nextbike.net/maps/gbfs/v2/nextbike_fg/gbfs.json)	GBFS is a standardised format used by many operators. The GBFS root lists feed URLs such as system_information, vehicle_types, station_information, station_status, free_bike_status and pricing
. station_information provides coordinates, capacity and other metadata for each station
, station_status provides number of bikes and docks available
, and free_bike_status returns each bike’s ID, reservation/disabled flags, GPS coordinates and battery percentage for electric bikes
.	Standard GBFS parameters such as system_id in the URL and optional language code (en, de etc.).	Medium – widely supported and standardised but lacks the board‑computer ID and detailed battery pack data found in the live feed.
Zone service (https://zone‑service.nextbikecloud.net/v1/zones/city/{cityId})	Returns a GeoJSON FeatureCollection describing service zones for a city. Each feature has a geometry with polygon coordinates and properties such as source, type (e.g., BusinessZone or FreeReturnZone), domain, cityId and an array of rules
.	cityId – the UID of the city, which can be obtained from the live feed.	Low – useful for mapping where bikes may be parked but contains no bike or station data.
Flexzone GeoJSON (https://api.nextbike.net/reservation/geojson/flexzone_{domain}.json)	Provides GeoJSON polygons outlining free‑return zones. Each feature includes visual styling (color and fill) and identifiers such as name, domain, cityId, category, flexzoneId and public flag
.	Domain (e.g., fg for Karlsruhe).	Low – only zone boundaries; no bike or station data.
Other endpoints	Nextbike’s internal API (e.g., /api/v1.1/, /oauth/) requires authentication and is used for rentals, bookings and user accounts. The public docs mention a key retrieval endpoint https://webview.nextbike.net/getAPIKey.json and other operations but those were not tested because they involve personal data and require user authentication
.	–	Not relevant for open data collection without credentials.
Notes on filtering and parameters
Domains and cities: Each network has a domain (string like fg for Karlsruhe or bn for Berlin). You can filter the live feed by passing domains=fg or city=21. The list_cities=1 parameter forces the response to include a cities list under each country.

Format selection: json delivers nested JSON, xml returns XML, and flatjson flattens the structure and compresses bike details. The default is json.

Geographic filters: Parameters lat and lng with distance limit results to a geographic radius. place and station accept place IDs to retrieve specific stations.

Omitting per‑bike data: Setting bikes=0 reduces response size by omitting the bike_list objects (only counts and numbers remain).

Comparison of data richness
The nextbike‑live feed is by far the most detailed of the public endpoints. For every active bike it exposes the internal board‑computer ID and battery status. In a Karlsruhe example, a single station’s bike_list contains objects like:

number – bike number or ID (e.g., 54502),

bike_type – numeric type code,

lock_types – array of lock types (usually frame_lock),

active and state – indicate if the bike is operable,

electric_lock – whether the lock is electronically controlled,

boardcomputer – unique identifier for the bike’s onboard computer,

pedelec_battery – battery percentage for pedelec bikes, and

battery_pack – object containing a percentage value
.

The official feed provides the same fields but only covers official stations. The flat JSON variant drops the bike_list object altogether, turning the bike numbers into comma‑separated strings and losing board‑computer and battery details
. The GBFS feeds standardise the data model across operators but are deliberately simplified: station_status only reports counts of bikes and docks
, while free_bike_status lists each bike’s ID, GPS coordinates and optional fuel percentage for e‑bikes
. None of the GBFS feeds expose the board‑computer or the detailed battery pack information.

Zone and flexzone endpoints supply geographic polygons for service zones and free‑return areas
. These datasets are valuable for mapping where bikes may be parked or returned, but they do not include any data about bikes or stations. When combined with the live feed they can be used to geofence bikes or visualise coverage.

Choosing the richest API
Given the available endpoints, the nextbike‑live.json API offers the most comprehensive data. It includes country, city and place metadata, counts of bikes, and—most importantly—per‑bike objects with board‑computer identifiers and battery status
. This level of detail enables:

tracking bikes’ availability and movement between stations,

analysing battery depletion patterns for electric bikes,

identifying station demand and supply based on counts and types of bikes, and

associating bikes with service events or maintenance using the board‑computer ID.

Therefore, the following documentation focuses on the nextbike‑live API.

Documentation for the nextbike‑live API
Base URL
https://maps.nextbike.net/maps/nextbike-live.{format}
Replace {format} with one of the following:

format	Description
json (default)	Nested JSON with full bike objects.
xml	Equivalent data as XML.
flatjson	Flat JSON where bikes are listed by number strings without per‑bike objects.
Common query parameters
Parameter	Example	Purpose
domains	domains=fg	Filter results by one or more domains (e.g., fg for Karlsruhe). Multiple domains can be separated by commas.
city	city=21	Filter by city ID. City IDs (UIDs) can be found in the cities list of the live feed.
countries	countries=de	Filter by ISO country codes.
place	place=458	Return data only for the specified place or station.
lat & lng	lat=49.0&lng=8.4&distance=1.0	Return entities within a radial distance (in kilometres) of the given coordinates.
list_cities	list_cities=1	Include the cities list under each country, which contains metadata such as uid, name, domain and counts
.
bikes	bikes=0	Omit the bike_list objects to reduce response size.
search	search=Hauptbahnhof	Full‑text search on place names (useful for UI queries).
Parameters may be combined. Omitting all filters returns data for every country and city, which can be very large.

Response structure
At the top level the API returns a countries array. Each country object includes fields such as name, domain, language, timezone, currency, available_bikes, and contact URLs. The Karlsruhe entry (domain fg) also includes counts like set_point_bikes and available_bikes
. Nested within a country is a cities array containing city objects with the following important properties:

uid – unique integer ID (e.g., 21 for Karlsruhe)
.

name and alias – human‑readable city names.

domain – network domain (fg).

num_places – number of places/stations in the city.

bounds – bounding box coordinates for the city
.

available_bikes – number of bikes currently available in the city
.

bike_types – dictionary of bike type codes and counts
.

Each city object contains a places array listing stations or free‑floating zones. For every place the following fields are available:

uid – unique identifier for the place.

lat, lng – latitude/longitude coordinates.

name – name of the station or zone.

spot – boolean; true for physical docking spots, false for virtual zones.

number – station number.

bikes – count of bikes present.

bikes_available_to_rent – number of bikes ready for rental.

bike_racks, free_racks – available rack counts (if applicable).

terminal_type – e.g., stele, free or empty.

bike_list – array of bike objects detailing each bike present
.

When bikes=1 (default), the bike_list provides per‑bike details:

Field	Description
number	Bike number or ID.
bike_type	Numeric type ID (e.g., 71 for standard bikes, 196 for e‑bikes).
lock_types	Array of lock types (e.g., frame_lock).
active / state	Flags indicating whether the bike is operational.
electric_lock	Indicates if the frame lock is electronically controlled.
boardcomputer	Unique board‑computer ID used internally by Nextbike
.
pedelec_battery	Battery percentage for pedelec bikes, if available
.
battery_pack	Object with a percentage value representing the remaining battery capacity
.
If a bike is an e‑bike or pedelec, both pedelec_battery and battery_pack may be present. Conventional bikes have null values for those fields.

Obtaining Karlsruhe data
To fetch data for Karlsruhe (domain fg), call:

https://maps.nextbike.net/maps/nextbike-live.json?domains=fg&list_cities=1
This returns the KVV.nextbike country entry with the Karlsruhe city object and all of its places and bikes
. Additional parameters like bikes=0 (to omit per‑bike objects) or lat/lng/distance (to limit the spatial extent) can be added if needed.

Using the API for data collection
Determine identifiers: Use the live feed once to identify the domain and city UID for Karlsruhe. In the example above, domain = fg and uid = 21
.

Fetch live data periodically: Poll https://maps.nextbike.net/maps/nextbike-live.json?domains=fg at regular intervals (e.g., every 5 minutes) to capture the status of all stations and bikes. Avoid high polling rates to respect server load; the refresh_rate field in the city object indicates how often the data updates (around 10 seconds for Karlsruhe)
.

Store snapshot data: For each poll, store the timestamp along with the full JSON response or a normalised version in a database. Using a spatial database such as PostgreSQL with PostGIS enables efficient queries on positions and zones.

Track bike movements: Because bikes have stable number and boardcomputer identifiers, you can compute movements by comparing locations across consecutive snapshots. When a bike appears in a different place or its latitude/longitude changes, record the trip. The state and battery fields allow filtering out bikes that are inactive or under maintenance.

Combine with zone data: Retrieve service zones via https://zone-service.nextbikecloud.net/v1/zones/city/21 and flex zones via https://api.nextbike.net/reservation/geojson/flexzone_fg.json to map out business areas and free‑return zones
. These GeoJSON features can be loaded into GIS software or a database for geospatial analysis.

Optionally use GBFS feeds: GBFS provides station capacities and standardised data that may complement the live feed. For example, use station_information to get rack capacities and free_bike_status for simplified lists of free bikes with fuel percentage
. However, GBFS lacks the board‑computer details available in the live feed.

Example Python snippet
The following snippet demonstrates how to fetch live data for Karlsruhe using Python. Replace the database logic with your preferred storage solution.

import requests
from datetime import datetime, timezone

BASE_URL = 'https://maps.nextbike.net/maps/nextbike-live.json'

def fetch_karlsruhe_data():
    params = {
        'domains': 'fg',   # Karlsruhe domain
        'list_cities': '1' # include cities metadata
    }
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    # attach a UTC timestamp for versioning
    data['fetched_at'] = datetime.now(timezone.utc).isoformat()
    return data

if __name__ == '__main__':
    snapshot = fetch_karlsruhe_data()
    # TODO: insert `snapshot` into your database (e.g. PostGIS or MongoDB)
    print('Fetched', len(snapshot.get('countries', [])), 'countries')
This code uses single quotes for strings and omits superfluous comments as requested. It attaches an ISO‑formatted timestamp to each snapshot to facilitate tracking changes over time.

Conclusion
Among the publicly accessible Nextbike endpoints, the nextbike‑live API provides the most detailed information. It delivers per‑bike objects with board‑computer and battery data
, enabling precise tracking of bikes and analysis of station loads. The official, flat JSON and GBFS feeds are useful but less detailed. The zone service and flexzone endpoints supply geographic boundaries for free‑return and business zones
. For a comprehensive data‑collection project aimed at generating statistics for city planning, I recommend polling the live feed for the relevant domain (fg) and combining it with zone and GBFS feeds where necessary. The documentation above should allow a developer to integrate the Nextbike live feed into a data pipeline without further research.
