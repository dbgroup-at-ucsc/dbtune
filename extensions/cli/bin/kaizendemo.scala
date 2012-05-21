var db = connect("jdbc:db2://192.168.56.101:50000/test", "db2inst1", "db2inst1admin")

// scenario 1

var workload = new WorkloadStream("workloads/kaizen-demo/scenario-1.sql");
var initialSet = db.recommend("SELECT col1 FROM one_table.tbl WHERE col1 = 2"); initialSet.addAll(db.recommend("SELECT col1 FROM one_table.tbl WHERE col2 = 2"))
var wfit = new WFIT(db, workload, initialSet)
plotTotalWork(workload, wfit.getOptimalRecommendationStatistics, wfit.getRecommendationStatistics)
showWFITTable(workload, wfit.getRecommendationStatistics)
workload.next
workload.next
workload.next
workload.next
workload.next
workload.next
workload.play
// maybe run online-benchmark
db.dropIndexes
resetUI

// scenario 2

// we could begin with the simple 3-index scenario
//var workload = new WorkloadStream("workloads/kaizen-demo/scenario-2.sql")
var workload = new WorkloadStream("workloads/tpch-10-counts/workload.sql")
var wfit1 = new WFIT(db, workload, 1)
var wfit100 = new WFIT(db, workload, 100)
plotTotalWork(workload, wfit1.getRecommendationStatistics, wfit100.getRecommendationStatistics)
showIndexTable(workload, wfit1.getRecommendationStatistics)
workload.next
workload.next
workload.next
workload.next
workload.play
showIndexTable(workload, wfit100.getRecommendationStatistics)
db.dropIndexes
resetUI

// scenario 3

var workload = new WorkloadStream("workloads/kaizen-demo/scenario-3-a.sql")
var wfit = new WFIT(db, workload)
var wfitGood = new WFIT(db, workload, "GOOD")
plotTotalWork(workload, wfit.getRecommendationStatistics, wfitGood.getRecommendationStatistics)
showIndexTable(workload, wfitGood.getRecommendationStatistics)
workload.next(4)
wfitGood.voteDown(1)
workload.next(4)
db.dropIndexes
resetUI

var workload = new WorkloadStream("workloads/kaizen-demo/scenario-3-b.sql")

workload.next(2)
wfitGood.voteDown(1)
workload.next(2)
workload.next
db.dropIndexes
resetUI
