package huristics;

import java.util.ArrayList;

import model.TripleEntry;
import model.TripleSlotEntry;
import Utils.DLogger;

public class PaperHuristics {
	final static DLogger L = new DLogger(true, "PaperHuristics");
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
	
	public static double calculateSim(TripleEntry triple1, TripleEntry triple2) {
		double sim = 0.0;
		
		double slotXsSim = PaperHuristics.calculateSim(triple1.slotXs, triple2.slotXs);
		double slotYsSim = PaperHuristics.calculateSim(triple1.slotYs, triple2.slotYs);
		
		try {
			sim = Math.sqrt(slotXsSim*slotYsSim);
		}
		catch (Exception e) {
			L.log("Failed to calculate sim " + e + ", " + triple1.path + ", " + triple2.path);
		}
		
		return sim;
	}
	
	public static double calculateSim(ArrayList<TripleSlotEntry> tripleSlots1, ArrayList<TripleSlotEntry> tripleSlots2) {
		double sim = 0.0;
		
		if (tripleSlots1.isEmpty() || tripleSlots2.isEmpty()) {
			L.log("One of the triples is empty ");
			return sim;
		}
		
		ArrayList<TripleSlotEntry> mutualSlot1 = new ArrayList<TripleSlotEntry>(tripleSlots1);
		ArrayList<TripleSlotEntry> mutualSlot2 = new ArrayList<TripleSlotEntry>(tripleSlots2);
		mutualSlot1.retainAll(tripleSlots2);
		mutualSlot2.retainAll(tripleSlots1);
		
		double sumMIOfMutualItems = 0.0;
		for (TripleSlotEntry entry : mutualSlot1) {
			sumMIOfMutualItems += entry.mi;
		}
		for (TripleSlotEntry entry : mutualSlot2) {
			sumMIOfMutualItems += entry.mi;
		}
		
		double sumOf1 = 0.0;
		for (TripleSlotEntry entry : tripleSlots1) {
			sumOf1 += entry.mi;
		}
		
		double sumOf2 = 0.0;
		for (TripleSlotEntry entry : tripleSlots2) {
			sumOf2 += entry.mi;
		}
		
		double divBy = sumOf1 + sumOf2;
		if (divBy != 0) {
			sim = sumMIOfMutualItems/divBy;
		}
		
		return sim;
	}
}
