/*
   Author:  David Abadir
   Use:  mongo <mongohost>:<port> [options] --shell  "path\to\script\mapReduceHelper.js"
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
