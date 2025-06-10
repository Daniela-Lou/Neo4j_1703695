//Exercici 3

//A
//1
CALL gds.wcc.stream({
  nodeProjection: ['Individual', 'House'],
  relationshipProjection: {
    LIVES_IN: {
      type: 'LIVES_IN',
      orientation: 'UNDIRECTED'
    }
  }
})
YIELD nodeId, componentId
WITH componentId, COUNT(nodeId) AS size
RETURN componentId, size
ORDER BY size DESC
LIMIT 10;

//2
MATCH (i:Individual)-[:LIVES_IN]->(h:House) RETURN h.municipality AS municipi, h.year_padron AS any, COUNT(*) AS num_parelles ORDER BY municipi, any;

//3
CALL gds.wcc.stream('individusHabitatges')
YIELD nodeId, componentId
WITH gds.util.asNode(nodeId) AS node, componentId 
WITH componentId, collect(DISTINCT labels(node)) AS tipusNodes
WHERE NOT any(etiqueta IN tipusNodes WHERE etiqueta = 'Habitatge') 
RETURN count(DISTINCT componentId) AS componentsSenseHabitatge

//B: 
//1
MATCH (h1:House), (h2:House)
WHERE h1.house_id = h2.house_id AND h1.year_padron < h2.year_padron
MERGE (h1)-[:MATEIX_HAB]->(h2);

//2
CALL gds.graph.project(
  'graf_individu_habitatge',
  ['Individual', 'House'],
  {
    LIVES_IN: {
      type: 'LIVES_IN',
      orientation: 'UNDIRECTED'
    },
    IS_FAMILY: {
      type: 'IS_FAMILY',
      orientation: 'UNDIRECTED'
    },
    MATEIX_HAB: {
      type: 'MATEIX_HAB',
      orientation: 'UNDIRECTED'
    }
  }
);

//3
CALL gds.nodeSimilarity.write({
  nodeProjection: ['Individual', 'House'],
  relationshipProjection: {
    LIVES_IN: {
      type: 'LIVES_IN',
      orientation: 'UNDIRECTED'
    },
    IS_FAMILY: {
      type: 'IS_FAMILY',
      orientation: 'UNDIRECTED'
    },
    MATEIX_HAB: {
      type: 'MATEIX_HAB',
      orientation: 'UNDIRECTED'
    }
  },
  writeRelationshipType: 'SIMILAR_TO',
  writeProperty: 'similarityScore'
});

