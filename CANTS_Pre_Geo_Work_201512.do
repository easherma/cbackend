* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* CANTS PRE GEO WORK - preps raw files for geocoding and spatial merging  *
* Ian Matthew Morey                                                       *
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

* This is a first draft of the script needed to prep the raw CANTS data
* I will follow Jeff's previous naming conventions for files and variables;
* however, I intend to change these in the second draft.

clear all
set more off

* * * * * CHANGE EACH QUARTER
local month1 "10"
local month2 "11"
local month3 "12"

local monthdays1 31
local monthdays2 30
local monthdays3 31

local year 2015


* * * * * SET LOCALS
local MYCODE /wd2/match/il/cants_geo/`year'`month3'/Code
local MYDATA /wd2/match/il/cants_geo/`year'`month3'/Data 
local MYSRCE `MYDATA'/source
local MYGEOG `MYDATA'/geo
local MYWORK `MYDATA'/work
local MYTABL `MYDATA'/relat_tables  

local _`year'`month1' "`year'`month1'`monthdays1'"
local _`year'`month2' "`year'`month2'`monthdays2'"
local _`year'`month3' "`year'`month3'`monthdays3'"

local QRTRMONTHS `month1' `month2' `month3'

* * * * * READ RAW FILES
foreach MONTH in `QRTRMONTHS' {

   * read in file using a fixed-width dictionary
   infile using `MYCODE'/CANTS_RawFile_Dictionary_`year'`month3'.dct, using(`MYSRCE'/tot_sub_cants_`year'`MONTH'.txt)
   
   * create indicator of file month using regex of file suffix
   gen month = "`MONTH'"
   
   * following Jeff's work
   gen effcdate = "`_`year'`MONTH''"
   gen zip4 = ""
   gen fips = ""
   
   replace fips="115" if cnty=="055" 
   replace fips="117" if cnty=="056" 
   replace fips="119" if cnty=="057" 
   replace fips="121" if cnty=="058" 
   replace fips="123" if cnty=="059" 
   replace fips="125" if cnty=="060" 
   replace fips="127" if cnty=="061" 
   replace fips="109" if cnty=="062" 
   replace fips="111" if cnty=="063" 
   replace fips="113" if cnty=="064" 
   replace fips="165" if cnty=="082"
   replace fips="167" if cnty=="083"
   replace fips="169" if cnty=="084" 
   replace fips="171" if cnty=="085" 
   replace fips="173" if cnty=="086" 
   replace fips="175" if cnty=="087" 
   replace fips="163" if cnty=="088" 
   replace fips="031" if cnty=="105" 
   replace fips="900" if cnty=="103" | cnty=="106"
   replace fips="217" if cnty=="104" 
   
      
   * clean
   replace zip="" if zip=="00000" | zip=="88888" | zip=="99999" 
   
   * rename vars, "o" suffix indicates original var before geo work
   rename street street_o
   rename city   city_o
   rename state  state_o
   rename zip    zip_o
   
   * save for use when building relational tables 
   save `MYWORK'/orig_cants_`year'`MONTH'.dta, replace
   
   * remove duplicates, save
   duplicates drop street_o city_o state_o zip_o, force
   save `MYWORK'/cants_`year'`MONTH'.dta, replace
    
   clear
}


* * * * * APPEND
use `MYWORK'/cants_`year'`month1'.dta
append using `MYWORK'/cants_`year'`month2'.dta
append using `MYWORK'/cants_`year'`month3'.dta

* remove duplicates for geocoding, spatial joins
duplicates drop street_o city_o state_o zip_o, force

* save for use when building relational tables
save `MYWORK'/original.dta, replace

* * * * * BEGIN CLEANING 
* generate working (hence "_w" suffix) vars, convert to upper, and trim excess space
foreach element in street city state zip {
   gen `element'_w = upper(`element'_o)
   replace `element'_w = trim(itrim(`element'_w)) 
}

* remove special characters
replace street_w = trim(itrim(subinstr(street_w, "#", "", .)))
replace street_w = trim(itrim(subinstr(street_w, ".", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "(", "", .)))
replace street_w = trim(itrim(subinstr(street_w, ")", "", .)))
replace street_w = trim(itrim(subinstr(street_w, ",", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "{", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "}", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "[", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "]", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "`", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "'", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "_", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "?", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "!", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "%", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "^", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "$", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "&", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "@", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "<", "", .)))
replace street_w = trim(itrim(subinstr(street_w, ">", "", .)))
replace street_w = trim(itrim(subinstr(street_w, "*", "", .)))
replace street_w = trim(itrim(subinstr(street_w, ":", "", .)))
replace street_w = trim(itrim(subinstr(street_w, ";", "", .)))
replace street_w = trim(itrim(subinstr(street_w, `"""', "", .)))



*** Apartment 
* subset to allow faster processing of regexs

preserve
keep if regexm(street_w, "AP(AR)?T(MENT)?")

* Apartment ### 
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?[ ]?[0-9]*$", "")))

* Apartment A-Z 
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?[ ]?[A-Z]?[A-Z]?$", "")))

* Apartment ### - A-Z
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?[ ]?[0-9]*[ ]?[-]*[ ]?[A-Z]?[A-Z]?$", "")))

* Apartment A-Z - ### 
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?[ ]?[A-Z]?[A-Z]?[ ]?[-]*[ ]?[0-9]*$", "")))

* Apartment ### - ### 
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?[ ]?[0-9]*[ ]?[-]*[ ]?[0-9]*$", "")))

* Apartment A-Z### - A-Z### 
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?[ ]?[A-Z]?[A-Z]?[ ]?[0-9]*[ ]?[-]*[ ]?[A-Z]?[A-Z]?[ ]?[0-9]*$", "")))

* Apartment ###A-Z - ###A-Z 
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?[ ]?[0-9]*[ ]?[A-Z]?[A-Z]?[ ]?[-]*[ ]?[0-9]*[ ]?[A-Z]?[A-Z]?$", "")))

* Apartment A-Z### - ###A-Z 
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?[ ]?[A-Z]?[A-Z]?[ ]?[0-9]*[ ]?[-]*[ ]?[0-9]*[ ]?[A-Z]?[A-Z]?$", "")))

* Apartment ###A-Z - A-Z### 
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?[ ]?[0-9]*[ ]?[A-Z]?[A-Z]?[ ]?[-]*[ ]?[A-Z]?[A-Z]?[ ]?[0-9]*$", "")))

* Apartment ###/### 
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?[ ]?[0-9]*[ ]?[/][ ]?[0-9]*$", "")))

* Apartment ### A-Z ###
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?[ ]?[0-9]*[ ]?[A-Z]?[A-Z]?[ ]?[0-9]*$", "")))

* Apartment ### Dir 
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?[ ]?[0-9]*[ ]?[-]*[ ]?(N(O?RTH)?|S(O?U?TH)?|E(AST)?|W(E?ST)?)[ ]?[0-9]*$", "")))

* Apartment ### ### FLOOR 
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?[ ]?([A-Z]?|[0-9]*)[ ]?[0-9]*[ ]?(ST|ND|RD|TH)?[ ]?(FL[O]*R?)?$", "")))

* Apartment ??? REAR/FRONT/BACK/etc
*(7 changes)
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?(.)*BA?SE?ME?N?T?$", "")))
*(13 changes)
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?(.)*F(L(O?O?R)?|RONT)$", "")))
*(3 changes)
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?(.)*GAR(AGE|DEN)$", "")))
*(14 changes)
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?(.)*REAR$", "")))
*(19 changes)
replace street_w = trim(itrim(regexr(street_w, "AP(AR)?T(MENT)?(.)*U(N?K?N?O?WN|P(PE?R|ST(AI)?R?S?))$", "")))

* ### Apartment A-Z ???
replace street_w = trim(itrim(regexs(1) + regexs(5))) if regexm(street_w, "^([0-9]*)[ ](AP(AR)?T(ME?NT)?[ ]?[A-Z])([ ](.)*)")

* ### Apartment ### ???
replace street_w = trim(itrim(regexs(1) + regexs(5))) if regexm(street_w, "^([0-9]*)[ ](AP(AR)?T(ME?NT)?[ ]?[0-9][0-9]?[0-9]?)([ ](.)*)")

* ### Apartment ### - A-Z ???
replace street_w = trim(itrim(regexs(1) + regexs(5))) if regexm(street_w, "^([0-9]*)[ ](AP(AR)?T(ME?NT)?[ ]?[0-9][0-9]?[0-9]?[ ]?[-]*[ ]?[A-Z])([ ](.)*)")

* ### Apartment  ### - 0-9 ???
replace street_w = trim(itrim(regexs(1) + regexs(5))) if regexm(street_w, "^([0-9]*)[ ](AP(AR)?T(ME?NT)?[ ]?[A-Z][ ]?[-]*[ ]?[0-9][0-9]?[0-9]?)([ ](.)*)")

* Apartment ### ### ???
replace street_w = trim(itrim(regexs(4))) if regexm(street_w, "^(AP(AR)?T(ME?NT)?[ ]?[0-9][0-9]?[0-9]?)([ ][0-9]*[ ]?[A-Z]*(.)*)")

* Apartment A-Z ### ???
replace street_w = trim(itrim(regexs(4))) if regexm(street_w, "^(AP(AR)?T(ME?NT)?[ ]?[A-Z])([ ][0-9][0-9]*[ ]?[A-Z]*(.)*)")

* save tempfile, restore, merge back on to main file
tempfile aptclean
save "`aptclean'"
restore
merge 1:1 individ street_o city_o state_o zip_o using `aptclean', update replace
drop _merge

* * * * * START: REGEX CLEANING SANDBOX
* there is still plenty of work that can be done to better clean the data
* * * * * END: REGEX CLEANING SANDBOX

* rename vars for geocoding (hence "_g" suffix)
foreach element in street city state zip {
   rename `element'_w `element'_g
}

* remove duplicates for geocoding, create a unique geoid for easy merge back
preserve

duplicates drop street_g city_g state_g zip_g, force
gen uniqgeoid = _n
tempfile uniqgeo
save "`uniqgeo'"

keep uniqgeoid street_g city_g state_g zip_g
save `MYGEOG'/cants_geoin_`year'`month3'.dta, replace

restore

* merge unique geoid back on to main file, save for post-geo-work
merge m:1 street_g city_g state_g zip_g using `uniqgeo', nogen
save `MYWORK'/cants_geoin_duplicategeoid_`year'`month3'.dta, replace






