# Airline Network Graph with Neo4j

A graph database project using Neo4j to analyze a flight network dataset (airports, airlines, routes) with Cypher.

## 🧩 Dataset

- Airports, airlines, and routes modeled as nodes and relationships.
- Imported from CSV using Neo4j tools.
- Indexed key properties to optimize performance.

## 📊 Queries Implemented

1. Top 5 airports by number of flights.
2. Top 5 countries by number of airports.
3. Top 5 airlines with international flights to/from Greece.
4. Top 5 airlines with domestic flights in Germany.
5. Top 10 countries with flights to Greece.
6. Air traffic percentage per Greek city.
7. Flights to Greece using plane types 738 and 320.
8. Top 5 longest flights by distance.
9. Cities not directly connected to Berlin (ranked by total flights).
10. All shortest paths from Athens to Sydney.

## 📂 Deliverables

- `Report.pdf`: Graph model, import steps, query results.
- `queries.cy`: All Cypher queries.
- Any scripts used for data loading or processing.
