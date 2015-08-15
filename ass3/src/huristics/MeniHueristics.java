package huristics;

import java.util.ArrayList;

import Utils.DLogger;
import model.TripleEntry;
import model.TripleSlotEntry;

public class MeniHueristics {
	final static DLogger L = new DLogger(true, "MeniHueristics");
	
	// Cosine
	
	public static double calculateTFIDF(String P_SLOT_W_STR, String WILD_SLOT_W_STR) {
		
		double P_SLOT_W = 0.0, WILD_SLOT_W = 0.0;
		try {
			P_SLOT_W = Double.parseDouble(P_SLOT_W_STR);
			WILD_SLOT_W = Double.parseDouble(WILD_SLOT_W_STR);
		}
		catch (Exception e) {
			L.log("Failed parsing number - " + e);
			return 0.0;
		}
		
		return P_SLOT_W / (1+Math.log(WILD_SLOT_W));
	}
	
	public static double calculateCosine(TripleEntry triple1, TripleEntry triple2) {
		double cosine = 0.0;
		
		double slotXsCos = MeniHueristics.calculateCosine(triple1.slotXs, triple2.slotXs);
		double slotYsCos = MeniHueristics.calculateCosine(triple1.slotYs, triple2.slotYs);
		
		try {
			cosine = Math.sqrt(slotXsCos*slotYsCos);
		}
		catch (Exception e) {
			L.log("Failed to calculate cosine " + e + ", " + triple1.path + ", " + triple2.path);
		}
		
		return cosine;
	}
	
	public static double calculateCosine(ArrayList<TripleSlotEntry> tripleSlots1, ArrayList<TripleSlotEntry> tripleSlots2) {
		double cos = 0.0;
		
		if (tripleSlots1.isEmpty() || tripleSlots2.isEmpty()) {
			L.log("One of the triples is empty ");
			return cos;
		}
		
		ArrayList<TripleSlotEntry> mutualSlot1 = new ArrayList<TripleSlotEntry>(tripleSlots1);
		ArrayList<TripleSlotEntry> mutualSlot2 = new ArrayList<TripleSlotEntry>(tripleSlots2);
		mutualSlot1.retainAll(tripleSlots2);
		mutualSlot2.retainAll(tripleSlots1);
		
		double sumTFIDFOfMutualItems = 0.0;
		for (TripleSlotEntry entry : mutualSlot1) {
			sumTFIDFOfMutualItems += entry.tfidf;
		}
		for (TripleSlotEntry entry : mutualSlot2) {
			sumTFIDFOfMutualItems += entry.tfidf;
		}
		
		double sumOf1 = 0.0;
		for (TripleSlotEntry entry : tripleSlots1) {
			sumOf1 += entry.tfidf;
		}
		
		double sumOf2 = 0.0;
		for (TripleSlotEntry entry : tripleSlots2) {
			sumOf2 += entry.tfidf;
		}
		
		double divBy = Math.sqrt(sumOf1*sumOf1) + Math.sqrt(sumOf2*sumOf2);
		if (divBy != 0) {
			cos = sumTFIDFOfMutualItems/divBy;
		}
		
		return cos;
	}
	
	// Cover
	
	public static double calculateDice(String P_SLOT_W_STR, String WILD_SLOT_W_STR, String P_SLOT_WILD_STR) {
		
		double P_SLOT_W = 0.0, WILD_SLOT_W = 0.0, P_SLOT_WILD = 0.0;
		try {
			P_SLOT_W = Double.parseDouble(P_SLOT_W_STR);
			WILD_SLOT_W = Double.parseDouble(WILD_SLOT_W_STR);
			P_SLOT_WILD = Double.parseDouble(P_SLOT_WILD_STR);
		}
		catch (Exception e) {
			L.log("Failed parsing number - " + e);
			return 0.0;
		}
		
		return (2*P_SLOT_W) / (WILD_SLOT_W + P_SLOT_WILD);
	}
	
	public static double calculateCover(TripleEntry triple1, TripleEntry triple2) {
		double cover = 0.0;
		
		double slotXsCover = MeniHueristics.calculateCover(triple1.slotXs, triple2.slotXs);
		double slotYsCover = MeniHueristics.calculateCover(triple1.slotYs, triple2.slotYs);
		
		try {
			cover = Math.sqrt(slotXsCover*slotYsCover);
		}
		catch (Exception e) {
			L.log("Failed to calculate cover " + e + ", " + triple1.path + ", " + triple2.path);
		}
		
		return cover;
	}
	
	public static double calculateCover(ArrayList<TripleSlotEntry> tripleSlots1, ArrayList<TripleSlotEntry> tripleSlots2) {
		double cov = 0.0;
		
		if (tripleSlots1.isEmpty() || tripleSlots2.isEmpty()) {
			L.log("One of the triples is empty ");
			return cov;
		}
		
		ArrayList<TripleSlotEntry> mutualSlot1 = new ArrayList<TripleSlotEntry>(tripleSlots1);
		ArrayList<TripleSlotEntry> mutualSlot2 = new ArrayList<TripleSlotEntry>(tripleSlots2);
		mutualSlot1.retainAll(tripleSlots2);
		mutualSlot2.retainAll(tripleSlots1);
		
		double sumDiceOfMutualItems = 0.0;
		for (TripleSlotEntry entry : mutualSlot1) {
			sumDiceOfMutualItems += entry.dice;
		}
		for (TripleSlotEntry entry : mutualSlot2) {
			sumDiceOfMutualItems += entry.dice;
		}
		
		double sumOf1 = 0.0;
		for (TripleSlotEntry entry : tripleSlots1) {
			sumOf1 += entry.dice;
		}
		
		double divBy = sumOf1;
		if (divBy != 0) {
			cov = sumDiceOfMutualItems/divBy;
		}
		
		return cov;
	}
}
