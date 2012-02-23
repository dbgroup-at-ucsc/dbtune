package interaction.util;

public class Debug {
	public static void println(String str) {
		System.out.println(str);
	}
	
	public static void println(Object obj) {
		println(obj.toString());
	}
}
