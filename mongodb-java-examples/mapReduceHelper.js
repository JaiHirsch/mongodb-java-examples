/*
   Author:  David Abadir
   Use:  mongo <mongohost>:<port> [options] --shell  "path\to\script\mapReduceHelper.js"
   
   Example output:
   
   mongos> progressMapReduce()                                                                                                  
   {                                                                                                                            
        "percentDone" : "77.59",                                                                                             
        "estimated_seconds_remaining" : "216683.85",                                                                         
        "estimated_complete_date" : "Sun Mar 30 2014 01:26:06 GMT-0500 (Central Daylight Time)",                             
        "msg" : "m/r: (1/3) emit phase M/R: (1/3) Emit Progress: 118530099/152770251 77%"                                    
   }                                                                                                                            
   {                                                                                                                            
        "percentDone" : "61.95",                                                                                             
        "estimated_seconds_remaining" : "265751.41",                                                                         
        "estimated_complete_date" : "Sun Mar 30 2014 15:03:54 GMT-0500 (Central Daylight Time)",                             
        "msg" : "m/r: (1/3) emit phase M/R: (1/3) Emit Progress: 94640299/152774026 61%"                                     
   }                                                                                                                            
   {                                                                                                                            
        "percentDone" : "35.10",                                                                                             
        "estimated_seconds_remaining" : "295193.73",                                                                         
        "estimated_complete_date" : "Sun Mar 30 2014 23:14:36 GMT-0500 (Central Daylight Time)",                             
        "msg" : "m/r: (1/3) emit phase M/R: (1/3) Emit Progress: 53645199/152849504 35%"                                     
  }                                                                                                                            

*/
var progressMapReduce=function(){
   db.currentOp().inprog.forEach(
      function(d){
         if(typeof(d.msg) != "undefined"){
            var percentDone =parseFloat(100 * d.progress.done/d.progress.total).toFixed(2);

            var secElapsed =  d.secs_running;
            var eta = (secElapsed * 100)/percentDone;

            var currentTime =new Date();
            var result = {percentDone: percentDone, estimated_seconds_remaining:eta.toFixed(2), estimated_complete_date:"" + new Date(currentTime.setSeconds(currentTime.getSeconds() + eta)), msg: d.msg};
            printjson(result);
         }
      }
   )
}
