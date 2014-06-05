from kartograph import Kartograph
import sys

 
cfg = {
    "layers": {
        "china": {
            "src": "bou2_4p.shp" ,
	    "simplify":2,
     
	    "attributes":{
		"ISO": "iso",
		"NAME_1": "name",
		"FIPS_1": "fips"}
		}
	},
	"bounds": {
		"mode": "bbox",
		"data": [80, 17, 125, 54],
		"padding": 0.06
	}  
 
 
}


K = Kartograph()
K.generate(cfg,  'map-china.svg', preview=True, format='svg')



 
 

 