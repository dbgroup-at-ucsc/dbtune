package edu.ucsc.dbtune.seq.utils;

import java.util.Date;

import edu.ucsc.dbtune.util.Rt;

/**
 * @author Rui Wang
 */
public class RTimer {
	public int interval = 10000;
	long start0;
	long start;
	long milestone = 0;
	long checkpoint = 0;
	public long finished = 0;
	public long currentCount = 0;
	long minMs = 1, maxMs = Long.MAX_VALUE;

	public RTimer() {
		start = start0 = System.currentTimeMillis();
	}

	public RTimer(long maxMileStone) {
		start = start0 = System.currentTimeMillis();
		this.maxMs = maxMileStone;
	}

	public void next() {
		next(null);
	}

	public static String getMemory() {
		long mem = Runtime.getRuntime().totalMemory();
		if (mem > 1024 * 1024 * 1024L) {
			return String.format("%.2fG", mem / (1024 * 1024 * 1024f));
		}
		if (mem > 1024 * 1024L) {
			return String.format("%.2fM", mem / (1024 * 1024f));
		}
		if (mem > 1024 * 1024 * 1024L) {
			return String.format("%.2fK", mem / 1024f);
		}
		return String.format("%,d", mem);
	}

	public long getCount() {
		return finished;
	}

	public void printInfo() {
		printInfo(System.currentTimeMillis() - start, null);
	}

	public void printInfo(long spend, String info) {
		System.out.format(Rt.getDate()
				+ " %,d\tspeed=%s\tmem=%s\ttime=%s\t%s\r\n", finished,
				formatSpeed(currentCount * 1000f / spend), getMemory(),
				formatTimeUsed(spend), info == null ? "" : info);
		start = System.currentTimeMillis();
		currentCount = 0;
	}

	public void next(String info) {
		finished++;
		currentCount++;
		if (true) {
			long spend = System.currentTimeMillis() - start;
			if (spend > interval) {
				printInfo(spend, info);
			}
			return;
		}
		if (milestone == 0) {
			if (System.currentTimeMillis() - start > 1000) {
				milestone = (long) Math
						.pow(10, Math.ceil(Math.log10(finished)));
				if (milestone < minMs)
					milestone = minMs;
				if (milestone > maxMs)
					milestone = maxMs;
				checkpoint = milestone / 1000;
				if (checkpoint < minMs)
					checkpoint = minMs;
				System.out.format("milestone is %,d\r\n", milestone);
			}
		} else {
			if (currentCount % milestone == 0) {
				long spend = System.currentTimeMillis() - start;
				System.out.format("%,d\t%,d\t%,dms\t%s\r\n", finished, Runtime
						.getRuntime().totalMemory(), spend, info == null ? ""
						: info);
				currentCount = 0;
				if (spend < 500 || spend > 15000) {
					long old = milestone;
					if (spend > 0) {
						milestone = (long) Math.pow(10, Math.ceil(Math
								.log10(milestone * 1000 / spend)));
					} else {
						milestone = maxMs;
					}
					if (milestone == 0)
						milestone = minMs;
					if (milestone > maxMs)
						milestone = maxMs;
					checkpoint = milestone / 1000;
					if (checkpoint < minMs)
						checkpoint = minMs;
					if (milestone != old)
						System.out.format("milestone changed to %,d\r\n",
								milestone);
				}
				start = System.currentTimeMillis();
			} else if (currentCount % checkpoint == 0) {
				long spend = System.currentTimeMillis() - start;
				if (spend > 1000) {
					long old = milestone;
					if (spend > 0) {
						milestone = (long) Math.pow(10, Math.ceil(Math
								.log10(currentCount)));
					} else {
						milestone = maxMs;
					}
					if (milestone == 0)
						milestone = minMs;
					if (milestone > maxMs)
						milestone = maxMs;
					checkpoint = milestone / 1000;
					if (checkpoint < minMs)
						checkpoint = minMs;
					if (milestone != old)
						System.out.format("milestone changed to %,d\r\n",
								milestone);
				}
			}
		}
	}

	public void next(String info, Object... args) {
		next(String.format(info, args));
	}

	public static String formatTimeUsed(long timeUsed) {
		return formatTimeUsedWithMilli(timeUsed);
	}

	public static String formatTimeUsedWithSecond(long timeUsed) {
		int milli = (int) (timeUsed % 1000);
		if (milli < 500)
			timeUsed -= milli;
		else
			timeUsed += (1000 - milli);
		String s = formatTimeUsedWithMilli(timeUsed);
		if (s.endsWith("s"))
			return s.substring(0, s.length() - 5) + "s";
		else
			return s.substring(0, s.length() - 4);
	}

	public static String formatTimeUsedWithMilli(long timeUsed) {
		boolean minus = false;
		if (timeUsed < 0) {
			timeUsed = -timeUsed;
			minus = true;
		}
		int hour = (int) (timeUsed / 3600000L);
		int min = (int) ((timeUsed % 3600000L) / 60000L);
		int sec = (int) ((timeUsed % 60000L) / 1000L);
		StringBuilder sb = new StringBuilder();
		if (hour > 0) {
			sb.append(hour);
			sb.append(":");
		}
		if (sb.length() > 0) {
			sb.append(String.format("%02d:", min));
		} else if (min > 0) {
			sb.append(min);
			sb.append(":");
		}
		if (sb.length() > 0)
			sb.append(String.format("%02d.%03d", sec, timeUsed % 1000));
		else
			sb.append(String.format("%d.%03d", sec, timeUsed % 1000));
		if (sb.length() <= 6)
			sb.append("s");
		if (minus)
			return "-" + sb.toString();
		else
			return sb.toString();
	}

	public static String formatSpeed(float speed) {
		if (speed > 1024 * 1024 * 1024)
			return String.format("%.2fG", speed / (1024 * 1024 * 1024));
		if (speed > 1024 * 1024)
			return String.format("%.2fM", speed / (1024 * 1024));
		if (speed > 1024)
			return String.format("%.2fK", speed / 1024);
		return String.format("%.2f", speed);
	}

	public void finish() {
		long timeUsed = System.currentTimeMillis() - start0;
		String s = timeUsed == 0 ? "0" : formatTimeUsed(timeUsed);
		String speed = timeUsed == 0 ? "unlimited" : formatSpeed(finished
				* 1000f / timeUsed);
		System.out.format("%s time=%s speed=%s\n", Rt.getInfo(), s, speed);
	}

	public void finishAndReset() {
		long timeUsed = System.currentTimeMillis() - start0;
		String s = timeUsed == 0 ? "0" : formatTimeUsed(timeUsed);
		String speed = timeUsed == 0 ? "unlimited" : formatSpeed(finished
				* 1000f / timeUsed);
		System.out.format("%s time=%s speed=%s\n", Rt.getInfo(), s, speed);
		reset();
	}

	public void finish(String info) {
		long timeUsed = System.currentTimeMillis() - start0;
		String s = timeUsed == 0 ? "0" : formatTimeUsed(timeUsed);
		String speed = timeUsed == 0 ? "unlimited" : formatSpeed(finished
				* 1000f / timeUsed);
		System.out.format("%s %s time=%s", Rt.getInfo(), info, s);
		if (finished > 0)
			System.out.format(" speed=%s\n", speed);
		System.out.println();
	}

	public void finishAuto(String info) {
		long timeUsed = System.currentTimeMillis() - start0;
		String s = timeUsed == 0 ? "0" : formatTimeUsed(timeUsed);
		String speed = timeUsed == 0 ? "unlimited" : formatSpeed(finished
				* 1000f / timeUsed);
		System.out.format("%s %s", Rt.getInfo(), info);
		if (timeUsed > 1000)
			System.out.format(" time=%s", s);
		if (finished > 0)
			System.out.format(" speed=%s\n", speed);
		System.out.println();
	}

	public void finish(String info, Object... args) {
		long timeUsed = System.currentTimeMillis() - start0;
		String s = timeUsed == 0 ? "0" : formatTimeUsed(timeUsed);
		String speed = timeUsed == 0 ? "unlimited" : formatSpeed(finished
				* 1000f / timeUsed);
		System.out.format("%s %s time=%s", Rt.getInfo(), String.format(info,
				args), s);
		if (finished > 0)
			System.out.format(" speed=%s\n", speed);
		System.out.println();
	}

	public void reset() {
		start0 = start = System.currentTimeMillis();
		finished = 0;
		currentCount = 0;
	}

	public void printSecondElapse(String info) {
		System.out.println(info + " : " + getSecondElapse() + "s");
	}

	public void printMilliSecondElapse(String info) {
		System.out.println(info + " : " + getMilliSecondElapse() + "ms");
	}

	public void printSecondElapse() {
		printSecondElapse("Time elapse");
	}

	public void printMilliSecondElapse() {
		printMilliSecondElapse("Time elapse");
	}

	public double getSecondElapse() {
		return getMilliSecondElapse() / 1000;
	}

	public double getMilliSecondElapse() {
		return (double) (System.currentTimeMillis() - start);
	}

	public long get() {
		return System.currentTimeMillis() - start;
	}

	public static void main(String[] args) throws Exception {
		RTimer timer = new RTimer();
		while (true) {
			String cmd = Rt.getStdCommand();
			if (cmd == null) {
				Thread.sleep(50);
				continue;
			}
			if ("r".equals(cmd))
				timer.reset();
			else if (cmd.length() == 0)
				timer.printMilliSecondElapse();
			else
				break;
		}
		// Rt.p(RTimer.formatTimeUsedWithSecond(-1000000));
		System.exit(0);
		for (int i = 0; i < 999; i++) {
			timer.next();
		}
		Thread.sleep(1000);
		timer.next();
		for (int i = 0; i < 10; i++) {
			timer.next();
			Thread.sleep(1000);
		}
		timer.finish();
	}
}
