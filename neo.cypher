// Projecte Neo4j script
LOAD CSV WITH HEADERS FROM 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTfU6oJBZhmhzzkV_0-avABPzHTdXy8851ySDbn2gq32WwaNmYxfiBtCGJGOZsMgCWjzlEGX4Zh1wqe/pub?output=csv' as row
with(row.Id) as id, toInteger(row.Year) as born, (row.name) as name, (row.surname) as surname1, (row.second_surname) as surname2
merge (i: Individu {id: id, born: born, name: name, surname1: surname1, surname2: surname2});

LOAD CSV WITH HEADERS FROM "https://docs.google.com/spreadsheets/d/e/2PACX-1vT0ZhR6BSO_M72JEmxXKs6GLuOwxm_Oy-0UruLJeX8_R04KAcICuvrwn2OENQhtuvddU5RSJSclHRJf/pub?output=csv" as row
with (row.Municipi) as municipi, toInteger(row.Id_Llar) as id_llar, toInteger(row.Any_Padro) as any_padro, (row.Carrer) as carrer, (row.Numero) as numero
where municipi <> "null"
merge(h: Habitatge{municipi: municipi, id_llar: id_llar, any_padro:any_padro, carrer: carrer, numero:numero}); 

LOAD CSV WITH HEADERS FROM 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRVOoMAMoxHiGboTjCIHo2yT30CCWgVHgocGnVJxiCTgyurtmqCfAFahHajobVzwXFLwhqajz1fqA8d/pub?output=csv' AS row
with(row.ID_1) as a, (row.Relacio_Harmonitzada) as relacio,(row.ID_2) as b
where relacio <> "null"
match (primer:Individu {id: a})
match (segon:Individu{id: b})
merge (primer)-[r:Familia {tipus: relacio}]->(segon); 

LOAD CSV WITH HEADERS FROM 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRM4DPeqFmv7w6kLH5msNk6_Hdh1wuExRirgysZKO_Q70L21MKBkDISIyjvdm8shVixl5Tcw_5zCfdg/pub?output=csv' AS row
with (row.IND) as indi, (row.Location) as muni, toInteger(row.Year) as year, toInteger(row.HOUSE_ID) as house_id
match (i:Individu {id: indi})
match (h:Habitatge {municipi: muni, id_llar: house_id, any_padro: year}) 
merge (i)-[r:Viu]->(h); 

LOAD CSV WITH HEADERS FROM 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTgC8TBmdXhjUOPKJxyiZSpetPYjaRC34gmxHj6H2AWvXTGbg7MLKVdJnwuh5bIeer7WLUi0OigI6wc/pub?output=csv' AS row
with (row.Id_A) as a, (row.Id_B) as b
match (n:Individu{id: a})
match (m:Individu{id: b})
merge (n)-[r:Same_as]->(m);


create constraint UniqueIndividuId for (m:Individu) require m.id is unique;
create constraint ExistFamiliaTipus for ()-[rel:Familia]->() require rel.tipus is not null;
create constraint UniqueHabitatgeMuniIdAny for (h:Habitatge) require (h.municipi, h.id_llar, h.any_padro) is node key;