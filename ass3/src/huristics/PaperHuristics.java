package huristics;

import Utils.DLogger;

public class PaperHuristics {
	final static DLogger L = new DLogger(true);
	public static double calculateMI(String p_slot_w_str, String wild_slot_wild_str,
			String p_slot_wild_str, String wild_slot_w_str) {
		
		double p_slot_w, wild_slot_wild, p_slot_wild ,wild_slot_w;
		try {
			p_slot_w = Double.parseDouble(p_slot_w_str);
			wild_slot_wild = Double.parseDouble(wild_slot_wild_str);
			p_slot_wild = Double.parseDouble(p_slot_wild_str);
			wild_slot_w = Double.parseDouble(wild_slot_w_str);
		}
		catch (Exception e) {
			L.log("Failed parsing number - " + e);
			return 0.0;
		}
		
		double beforeLog = (p_slot_w*wild_slot_wild)/(p_slot_wild*wild_slot_w);
		double output = Math.log(beforeLog);
		
		return output;
	}
}
