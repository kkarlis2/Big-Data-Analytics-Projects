// Καθαρισμός υπαρχόντων δεδομένων 
MATCH (n) DETACH DELETE n;

// Δημιουργία περιορισμών
CREATE CONSTRAINT airport_id IF NOT EXISTS FOR (a:Airport) REQUIRE a.id IS UNIQUE;
CREATE CONSTRAINT airline_id IF NOT EXISTS FOR (al:Airline) REQUIRE al.id IS UNIQUE;
CREATE CONSTRAINT city_compound_key IF NOT EXISTS FOR (c:City) REQUIRE (c.name, c.country_name) IS UNIQUE;
CREATE CONSTRAINT country_name IF NOT EXISTS FOR (c:Country) REQUIRE c.name IS UNIQUE;
CREATE CONSTRAINT route_id IF NOT EXISTS FOR (r:Route) REQUIRE r.id IS UNIQUE;

// Εισαγωγή Countries
LOAD CSV WITH HEADERS FROM 'file:///airports.csv' AS row 
MERGE (c:Country {name: row.Country});

// Εισαγωγή Cities
LOAD CSV WITH HEADERS FROM 'file:///airports.csv' AS row
MERGE (city:City {name: row.City, country_name: row.Country})
WITH city, row
MATCH (country:Country {name: row.Country})
MERGE (city)-[:BELONGS_TO]->(country);

// Εισαγωγή Airports
LOAD CSV WITH HEADERS FROM 'file:///airports.csv' AS row
MERGE (a:Airport {id: toInteger(row.AirportID)})
SET a.name = row.Name,
    a.iata = row.IATA,
    a.icao = row.ICAO,
    a.latitude = toFloat(row.Latitude),
    a.longitude = toFloat(row.Longitude),
    a.altitude = toInteger(row.Altitude),
    a.timezone = toFloat(row.Timezone)
WITH a, row
MATCH (city:City {name: row.City, country_name: row.Country})
MERGE (a)-[:LOCATED_IN]->(city);

// Εισαγωγή Airlines
LOAD CSV WITH HEADERS FROM 'file:///airlines.csv' AS row
MERGE (al:Airline {id: toInteger(row.AirlineID)})
SET al.name = row.Name,
    al.alias = row.Alias,
    al.iata = row.IATA,
    al.icao = row.ICAO,
    al.callsign = row.Callsign,
    al.active = row.Active
WITH al, row
MATCH (country:Country {name: row.Country})
MERGE (al)-[:BASED_IN]->(country);

// Εισαγωγή Routes
LOAD CSV WITH HEADERS FROM 'file:///routes.csv' AS row
WITH row, (row.AirlineID + '_' + row.SourceID + '_' + row.DestinationID) AS routeId
MERGE (route:Route {id: routeId})
SET route.stops = toInteger(CASE row.Stops WHEN null THEN '0' ELSE row.Stops END),
    route.codeshare = CASE row.Codeshare WHEN null THEN '' ELSE row.Codeshare END,
    route.equipment = CASE row.Equipment WHEN null THEN '' ELSE row.Equipment END
WITH route, row
MATCH (al:Airline {id: toInteger(row.AirlineID)})
MATCH (src:Airport {id: toInteger(row.SourceID)})
MATCH (dst:Airport {id: toInteger(row.DestinationID)})
MERGE (al)-[:OPERATES]->(route)
MERGE (src)-[:HAS_DEPARTURE]->(route)
MERGE (route)-[:HAS_ARRIVAL]->(dst);



//erwtima 1 
MATCH (a:Airport)-[:HAS_DEPARTURE|HAS_ARRIVAL]-(r:Route)
WITH a.name as airport_name, COUNT(r) as total_flights
RETURN airport_name, total_flights
ORDER BY total_flights DESC
LIMIT 5;

//erwtima 2
MATCH (c:Country)<-[:BELONGS_TO]-(city:City)<-[:LOCATED_IN]-(a:Airport)
WITH c.name as country_name, COUNT(DISTINCT a.id) as airport_count
WHERE country_name IS NOT NULL
RETURN country_name, airport_count
ORDER BY airport_count DESC
LIMIT 5;

//erwtima 3 
MATCH (al:Airline)-[:OPERATES]->(r:Route),
      (src:Airport)-[:HAS_DEPARTURE]->(r),
      (r)-[:HAS_ARRIVAL]->(dst:Airport),
      (src)-[:LOCATED_IN]->(srcCity:City)-[:BELONGS_TO]->(srcCountry:Country),
      (dst)-[:LOCATED_IN]->(dstCity:City)-[:BELONGS_TO]->(dstCountry:Country)
WHERE (srcCountry.name = 'Greece' AND dstCountry.name <> 'Greece')
   OR (dstCountry.name = 'Greece' AND srcCountry.name <> 'Greece')
WITH al.name as airline_name, COUNT(DISTINCT r.id) as total_flights
WHERE airline_name IS NOT NULL
RETURN airline_name, total_flights
ORDER BY total_flights DESC
LIMIT 5;

//erwtima 4 
MATCH (al:Airline)-[:OPERATES]->(r:Route),
      (src:Airport)-[:HAS_DEPARTURE]->(r),
      (r)-[:HAS_ARRIVAL]->(dst:Airport),
      (src)-[:LOCATED_IN]->(srcCity:City)-[:BELONGS_TO]->(srcCountry:Country),
      (dst)-[:LOCATED_IN]->(dstCity:City)-[:BELONGS_TO]->(dstCountry:Country)
WHERE srcCountry.name = 'Germany' 
  AND dstCountry.name = 'Germany'
WITH al.name as airline_name, COUNT(DISTINCT r.id) as total_flights
WHERE airline_name IS NOT NULL
RETURN airline_name, total_flights
ORDER BY total_flights DESC
LIMIT 5;

//erwtima 5
//Εδω εμφανίζουμε 11 κατηγορίες καθως μετράμε για πτήσεις προς την Ελλάδα και τις εσωτερικές πτήσεις.
MATCH (al:Airline)-[:OPERATES]->(r:Route),
      (src:Airport)-[:HAS_DEPARTURE]->(r),
      (r)-[:HAS_ARRIVAL]->(dst:Airport),
      (src)-[:LOCATED_IN]->(srcCity:City)-[:BELONGS_TO]->(srcCountry:Country),
      (dst)-[:LOCATED_IN]->(dstCity:City)-[:BELONGS_TO]->(dstCountry:Country)
WHERE dstCountry.name = 'Greece'
WITH srcCountry.name as country_name, COUNT(DISTINCT r.id) as total_flights
WHERE country_name IS NOT NULL
RETURN country_name, total_flights
ORDER BY total_flights DESC
LIMIT 11;

//erwtima 6
//στο screenshot δεν χωρουσαν ολες οι χωρες και πηραμε κάποιες.
MATCH (city:City)-[:BELONGS_TO]->(country:Country {name: 'Greece'})
WITH city

OPTIONAL MATCH (city)<-[:LOCATED_IN]-(airport:Airport)
OPTIONAL MATCH (airport)-[:HAS_DEPARTURE|HAS_ARRIVAL]-(route:Route)

WITH city.name as city_name, COUNT(DISTINCT route) as city_traffic

WITH COLLECT({city: city_name, traffic: city_traffic}) as cities,
     SUM(city_traffic) as total_traffic

UNWIND cities as city_data
RETURN city_data.city as city_name,
       CASE
         WHEN total_traffic > 0 THEN round(100.0 * city_data.traffic / total_traffic, 2)
         ELSE 0
       END as traffic_percentage
ORDER BY traffic_percentage DESC;

//erwtima 7 
// Βρίσκουμε πτήσεις που καταλήγουν σε ελληνικά αεροδρόμια
MATCH (sourceAirport:Airport)-[:LOCATED_IN]->(sourceCity:City)-[:BELONGS_TO]->(sourceCountry:Country),
      (destAirport:Airport)-[:LOCATED_IN]->(:City)-[:BELONGS_TO]->(destCountry:Country {name: 'Greece'}),
      (sourceAirport)-[:HAS_DEPARTURE]->(route:Route)-[:HAS_ARRIVAL]->(destAirport)
// Βεβαιωνόμαστε ότι η πτήση είναι διεθνής (διαφορετικές χώρες)
WHERE sourceCountry.name <> 'Greece'
  AND route.equipment IN ['738', '320']
// Επιστρέφουμε τον τύπο αεροπλάνου και το πλήθος πτήσεων
RETURN route.equipment as aircraft_type,
       COUNT(route) as number_of_flights
ORDER BY aircraft_type;

//erwtima 8
MATCH (src:Airport)-[:HAS_DEPARTURE]->(r:Route)-[:HAS_ARRIVAL]->(dst:Airport)
WITH src, dst,
point({ longitude: src.longitude, latitude: src.latitude }) as p1,
point({ longitude: dst.longitude, latitude: dst.latitude }) as p2
WITH src.name as from_airport, 
     dst.name as to_airport, 
     round(point.distance(p1, p2)/1000*100)/100 as distance_km
RETURN from_airport, to_airport, distance_km
ORDER BY distance_km DESC
LIMIT 5;

//erwtima 9
MATCH (c:City)
WHERE NOT EXISTS {
    // Ελέγχουμε ΜΟΝΟ για απευθείας πτήσεις προς Βερολίνο
    MATCH (c)<-[:LOCATED_IN]-(a:Airport)
    -[:HAS_DEPARTURE]->(:Route)-[:HAS_ARRIVAL]->
    (:Airport)-[:LOCATED_IN]->(:City {name: 'Berlin'})
}
WITH c
MATCH (c)<-[:LOCATED_IN]-(airport:Airport)
-[:HAS_DEPARTURE]->(route:Route)
WITH c.name as city_name, COUNT(DISTINCT route) as total_flights
WHERE total_flights > 0
RETURN city_name, total_flights
ORDER BY total_flights DESC
LIMIT 5;

//erwtima 10
MATCH (start:Airport)-[:LOCATED_IN]->(startCity:City {name: 'Athens'}),
      (end:Airport)-[:LOCATED_IN]->(endCity:City {name: 'Sydney'})
MATCH p = shortestPath(
    (start)-[:HAS_DEPARTURE|HAS_ARRIVAL*]-(end)
)
RETURN [node in nodes(p) WHERE node:Airport | node.name] as ShortestPath,
       length(p)/2 as PathLength
ORDER BY PathLength;