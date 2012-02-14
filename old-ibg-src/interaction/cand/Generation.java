package interaction.cand;

public class Generation {
	public enum Strategy {
		UNION_OPTIMAL("optimal"), // take the union of all optimal index sets per query
		OPTIMAL_1C("1C"), // take union of all optimal indexes and just use single columns
		FULL_BUDGET("full"), // use the advisor with an infinite budget
		HALF_BUDGET("half"), // use advisor with half the budget used by full case
		POWER_SET("powerset");
		
		public final String nickname;
		
		Strategy(String n) {
			nickname = n;
		}
	}
}