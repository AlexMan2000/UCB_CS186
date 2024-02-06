-- Before running drop any existing views
DROP VIEW IF EXISTS q0;
DROP VIEW IF EXISTS q1i;
DROP VIEW IF EXISTS q1ii;
DROP VIEW IF EXISTS q1iii;
DROP VIEW IF EXISTS q1iv;
DROP VIEW IF EXISTS q2i;
DROP VIEW IF EXISTS q2ii;
DROP VIEW IF EXISTS q2iii;
DROP VIEW IF EXISTS q3i;
DROP VIEW IF EXISTS q3ii;
DROP VIEW IF EXISTS q3iii;
DROP VIEW IF EXISTS q4i;
DROP VIEW IF EXISTS q4ii;
DROP VIEW IF EXISTS q4iii;
DROP VIEW IF EXISTS q4iv;
DROP VIEW IF EXISTS q4v;

-- Question 0
CREATE VIEW q0(era)
AS
  -- solution you provide
	SELECT MAX(era)
	FROM pitching;
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT nameFirst, nameLast, birthYear 
  FROM people
  WHERE weight > 300;
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT nameFirst, nameLast, birthYear 
  FROM people
  WHERE nameFirst LIKE '% %'
  ORDER BY nameFirst ASC, nameLast ASC;
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthYear, AVG(height), COUNT(*)
  FROM people 
  GROUP BY birthYear
  ORDER BY birthYear ASC;
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthYear, AVG(height), COUNT(*)
  FROM people 
  GROUP BY birthYear 
  HAVING AVG(height) > 70
  ORDER BY birthYear ASC;
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT p.nameFirst, p.nameLast, p.playerID, h.yearid 
  FROM people as p INNER JOIN HallofFame as h
  ON p.playerID = h.playerID
  WHERE h.inducted == 'Y'
  ORDER BY h.yearid DESC, p.playerid ASC
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS

  -- Here one player could be appearing in the same school multiple years
  -- For test, we don't have to add DISTINCT here.
  WITH playerSchool(playerID, schoolID) AS
  (SELECT c.playerID, c.schoolID
  FROM CollegePlaying AS c LEFT JOIN schools AS s
  ON c.schoolID = s.schoolID
  WHERE s.schoolState == 'CA')
  
  
  SELECT p.nameFirst, p.nameLast, p.playerID, ps.schoolID, h.yearid 
  FROM people AS p INNER JOIN HallofFame AS h
  ON p.playerID = h.playerID
  INNER JOIN playerSchool AS ps 
  ON p.playerID = ps.playerID
  WHERE h.inducted == 'Y'
  ORDER BY h.yearid DESC, ps.schoolID ASC, p.playerID ASC
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS

  WITH playerSchool(playerID, schoolID) AS
  (SELECT c.playerID, c.schoolID
  FROM CollegePlaying AS c LEFT JOIN schools AS s
  ON c.schoolID = s.schoolID),
  
  playerHall(nameFirst, nameLast, playerID) AS
  (SELECT p.nameFirst, p.nameLast, p.playerID
  FROM people AS p INNER JOIN HallofFame AS h
  ON p.playerID = h.playerID
  WHERE h.inducted == 'Y')
  
  SELECT ph.playerID, ph.nameFirst, ph.nameLast, ps.schoolID
  FROM playerHall AS ph LEFT JOIN playerSchool AS ps
  ON ph.playerID = ps.playerID
  ORDER BY ph.playerID DESC, ps.schoolID ASC
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT p.playerID, p.nameFirst, p.nameLast, b.yearID
  , CAST(b.H + b.H2B + 2 * b.H3B + 3 * b.HR AS FLOAT) / AB AS slg
  FROM people AS p 
  INNER JOIN Batting AS b
  ON p.playerID = b.playerID
  WHERE b.AB > 50
  ORDER BY slg DESC, b.yearID ASC, p.playerID ASC
  LIMIT 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  WITH LTSLG(playerID, LH, LAB, LH2B, LH3B, LHR) AS
  (SELECT playerID, SUM(H) AS LH
  , SUM(AB) AS LAB
  , SUM(H2B) AS LH2B
  , SUM(H3B) AS LH3B
  , SUM(HR) AS LHR 
  FROM Batting 
  GROUP BY playerID 
  HAVING SUM(AB) > 50
  )
  
  SELECT p.playerID, p.nameFirst, p.nameLast 
  , CAST(b.LH + b.LH2B + 2 * b.LH3B + 3 * b.LHR AS FLOAT) / LAB AS lslg
  FROM people AS p 
  INNER JOIN LTSLG AS b
  ON p.playerID = b.playerID
  ORDER BY lslg DESC, p.playerID ASC
  LIMIT 10
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  WITH LTSLG(playerID, LSLG) AS
  (SELECT playerID
  , (CAST(SUM(H) + SUM(H2B) + 2 * SUM(H3B) + 3 * SUM(HR) AS FLOAT)) / SUM(AB) AS LSLG
  FROM Batting
  GROUP BY playerID 
  HAVING SUM(AB) > 50
  )
  
  SELECT  p.nameFirst, p.nameLast, b.LSLG AS lslg
  FROM people AS p 
  INNER JOIN LTSLG AS b
  ON p.playerID = b.playerID
  WHERE LSLG > (SELECT LSLG FROM LTSLG WHERE playerID == 'mayswi01')
  ORDER BY LSLG DESC, p.playerID ASC
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg)
AS
  SELECT yearID, MIN(salary) AS min, MAX(salary) AS max, AVG(salary) AS avg
  FROM salaries 
  GROUP BY yearID
  ORDER BY yearID ASC
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS

  WITH salaryIn2016(salary) AS
  (SELECT salary 
  FROM salaries 
  WHERE yearID == 2016),

  binWidth(binW) AS 
  (SELECT (MAX(salary) - MIN(salary)) / (SELECT count(binid) FROM binids) AS binW
  FROM salaryIn2016),
  
  salaryToBin(salary, binid) AS
  (SELECT salary
  ,CASE WHEN salary == (SELECT MAX(salary) from salaryIn2016) 
		THEN CAST((salary - (SELECT MIN(salary) FROM salaryIn2016)) / (SELECT binW from binWidth) AS INT) - 1
        ELSE CAST((salary - (SELECT MIN(salary) FROM salaryIn2016)) / (SELECT binW from binWidth) AS INT)
		END 
	AS binid
  FROM salaryIn2016)
  
  SELECT b.binid
  , (SELECT MIN(salary) FROM salaryIn2016) + b.binid * (SELECT binW from binWidth)
  , (SELECT MIN(salary) FROM salaryIn2016) + (b.binid + 1) * (SELECT binW from binWidth)
  , count(salary) AS count 
  FROM binids AS b 
  LEFT JOIN salaryToBin AS mapping
  ON mapping.binid = b.binid
  GROUP BY b.binid
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  WITH yearlyStat(yearID, minSalary, maxSalary, avgSalary) AS
  (SELECT yearID, MIN(salary), MAX(salary), AVG(salary)
  FROM salaries
  GROUP BY yearID
  )
  
  SELECT a.yearID
  ,a.minSalary - (SELECT b.minSalary FROM yearlyStat as b WHERE b.yearID + 1 = a.yearID)
  ,a.maxSalary - (SELECT b.maxSalary FROM yearlyStat as b WHERE b.yearID + 1 = a.yearID)
  ,a.avgSalary - (SELECT b.avgSalary FROM yearlyStat as b WHERE b.yearID + 1 = a.yearID)
  FROM yearlyStat AS a
  WHERE a.yearID != (SELECT MIN(yearID) FROM yearlyStat)
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  WITH maxYearlySalary(yearID, maxSalary) AS
  (SELECT yearID, MAX(salary)
  FROM salaries 
  GROUP BY yearID)
  
  SELECT p.playerID, p.nameFirst, p.nameLast, s.salary, s.yearID
  FROM people as p
  LEFT JOIN salaries as s
  ON p.playerID = s.playerID
  WHERE (s.yearID = 2000 and s.salary = (SELECT maxSalary FROM maxYearlySalary WHERE yearID = 2000))
  OR (s.yearId = 2001 and s.salary = (SELECT maxSalary FROM maxYearlySalary WHERE yearID = 2001))
;


-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS

  SELECT a.teamid, max(salary) - min(salary)
  FROM allstarfull as a
  INNER JOIN salaries as s 
  ON a.playerid = s.playerid and a.yearid = s.yearid 
  WHERE a.yearid = 2016
  GROUP BY a.teamid
;

