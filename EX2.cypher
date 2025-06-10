//Exercici 2

//A

MATCH (i:Individu)-[:Viu]->(h:Habitatge)
WHERE h.municipi = "CR" 
  AND i.surname1 IS NOT NULL 
  AND i.surname1 <> "nan"
RETURN 
  h.any_padro AS AnyPadro,
  COUNT(DISTINCT i.id) AS NombreHabitants,
  COLLECT(DISTINCT i.surname1) AS Cognoms;


//B:

MATCH (p:Individu) -[:Viu] ->(h:Habitatge{any_padro:1838, municipi:"SFLL"}) <-[:Viu]- (s:Individu{name:"rafel", surname1:"marti"}) 
RETURN p, h, s

MATCH (p:Individu) -[:Viu] ->(h:Habitatge{any_padro:1838, municipi:"SFLL"}) <-[:Viu]- (s:Individu{name:"rafel", surname1:"marti"}) 
RETURN h.numero, collect(p.name) as familiares, s.name as jefe


//C:   

match (n:Individu{name:"miguel", surname1:"estape", surname2:"bofill"}) -[:Same_as]- (p:Individu)
return count(n), collect(distinct p.name) as noms, collect(distinct p.surname1) as cognoms, collect(distinct p.surname2) as segons_cognoms


//D:
    
MATCH (h:Habitatge {municipi: "SFLL", any_padro: 1881})<-[:Viu]-(p:Individu)<-[:Familia {tipus: "jefe"}]-(j:Individu)-[r:Familia]-(f:Individu)
WHERE r.tipus in ["fill", "filla"]
WITH h, count(f) as fills_per_llar
RETURN count(h) as llars,sum(fills_per_llar) as total_fills, avg(fills_per_llar) as fills_mitjans_per_habitatge


//E: 

match (h:Habitatge {municipi: "CR"})<-[:Viu]-(p:Individu)<-[:Familia {tipus: "jefe"}]-(j:Individu)-[r:Familia]-(f:Individu) 
where r.tipus in ["fill", "filla"]
with count(f) as fills, j, collect(f.name) as nombres where fills >3 
return j.name, j.surname1, j.surname2, nombres, fills order by fills desc limit 20
    

//F:
    
MATCH (h:Habitatge {municipi: 'SFLL'})<-[:Viu]-(i:Individu)
WITH h.any_padro AS any, h.carrer AS carrer, COUNT(i) AS habitants

CALL {
  MATCH (h2:Habitatge {municipi: 'SFLL'})<-[:Viu]-(i2:Individu)
  WITH h2.any_padro AS any_sub, h2.carrer AS carrer_sub, COUNT(i2) AS habitants_sub
  RETURN any_sub, MIN(habitants_sub) AS min_habitants
}

WITH any, carrer, habitants, any_sub, min_habitants
WHERE any = any_sub AND habitants = min_habitants

RETURN any AS AnyPadro, carrer AS CarrerAmbMenysHabitants, habitants AS NombreHabitants
ORDER BY AnyPadro ASC

    


