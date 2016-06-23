* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* CANTS POST GEO WORK - redacts files post geocoding and spatial merging  *
* Ian Matthew Morey                                                       *
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

clear all


* * * * * CHANGE EACH QUARTER
local month1 "10"
local month2 "11"
local month3 "12"
local year   2015

* * * * * SET LOCALS
local MYCODE /wd2/match/il/cants_geo/`year'`month3'/Code
local MYDATA /wd2/match/il/cants_geo/`year'`month3'/Data 
local MYSRCE `MYDATA'/source
local MYGEOG `MYDATA'/geo
local MYWORK `MYDATA'/work
local MYTABL `MYDATA'/relat_tables  

local MYJOIN MCD2010 Place Tract10 CCA

local MCD2010   "COUSUBFP10"
local Place     "placefp"
local Tract10   "TRACTCE10 GEOID10 STATEFP10 COUNTYFP10"
local CCA       "AREA_NUMBE community"


* * * * * BRING IN XY AND MATCH STATUS DATA
use `MYGEOG'/geocode_result.dta, clear
keep uniqgeoid Status x y
order uniqgeoid, first


* * * * * BRING IN SPATIALLY MERGED DATA
preserve

foreach JOIN in `MYJOIN' {
   use `MYGEOG'/`JOIN'_Join_Output.dta, clear
   keep uniqgeoid ``JOIN''
   tempfile `JOIN'_Data
   save "``JOIN'_Data'"
}

restore

* merge joins to xy data
foreach JOIN in `MYJOIN' {
   merge 1:1 uniqgeoid using ``JOIN'_Data', nogen
}

* clean variable names and labels
rename x          longitude
rename y          latitude
rename TRACTCE10  tract
rename COUNTYFP10 fipscounty
rename AREA_NUMBE ccanum
rename STATEFP10  fipsstate
rename GEOID10    fipsall
rename placefp    place
rename COUSUBFP10 mcd2000
rename Status     status


* * * * * MERGE GEO WORK WITH DUPLICATED PRE-GEO WORK DATA
merge 1:m uniqgeoid using `MYWORK'/cants_geoin_duplicategeoid_`year'`month3'.dta, nogen

* save before merge with original data
tempfile geo_dupl
save "`geo_dupl'"


* * * * * APPEND THREE ORIGINAL MONTH FILES WITH DUPLICATED GEO DATA
use `MYWORK'/orig_cants_`year'`month1'.dta, clear
append using `MYWORK'/orig_cants_`year'`month2'.dta
append using `MYWORK'/orig_cants_`year'`month3'.dta

* bring in duplicated geo data
merge m:1 street_o city_o state_o zip_o using "`geo_dupl'", nogen

* rename to fit Jeff's code
replace community = trim(itrim(community))
rename community commarea

* sort including month to match the output of Jeff's file where majority of obs come from the first file
sort street_o city_o state_o zip_o month
drop month uniqgeoid
duplicates drop street_o city_o state_o zip_o, force


* * * * * SAVE RESEARCH READY GEO DATA FILE
save `MYWORK'/cants_geodata_`year'`month3'.dta, replace





