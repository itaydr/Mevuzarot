package model;

import Utils.DLogger;


public class NGramFactory {
	
	/**
	 *  Main array  - 
	 *  	head_word<TAB>syntactic-ngram<TAB>total_count<TAB>counts_by_year
	 *  Example - 
	 *  	experience      that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0      3092
	 *  Syntactic biarcs NGram -
	 *  	that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0
	 */
	final static int NUMER_OF_OBJECTS_IN_MAIN_ARRAY = 4;
	final static int TOTAL_COUNT_INDEX = 2;
	final static int NGRAM_INDEX = 1;
	final static int MIN_SYNTACTIC_NGRAM_LENGTH = 3;
	final static DLogger L = new DLogger(true);
		
	public static NGram parseNGram(String ngramStr) {
		
		if (ngramStr == null) {
			L.log("Cannot parse null string.");
			return null;
		}
		
		String[] mainArr = ngramStr.split("\t");
		
		if (mainArr.length != NUMER_OF_OBJECTS_IN_MAIN_ARRAY) {
			L.log("Less then " + NUMER_OF_OBJECTS_IN_MAIN_ARRAY + " objcts in the NGram array: " + ngramStr);
			return null;
		}
		
		String countStr = mainArr[TOTAL_COUNT_INDEX];
		double count = 0;
		try {
			count = Double.parseDouble(countStr);
		}
		catch (Exception e) {L.log("Failed to parse count: " + countStr + " for ngram: " + ngramStr);}
		
		String syntacticNgram = mainArr[NGRAM_INDEX];
		String[] syntacticNGramArr = syntacticNgram.split(" ");
		
		if (syntacticNGramArr.length < MIN_SYNTACTIC_NGRAM_LENGTH) {
			L.log("Too few arguments in syntactic ngram: ." + syntacticNgram);
			return null;
		}
		
		String slotX = slotXFromSyntacticNGramStr(syntacticNGramArr);
		String slotY = slotYFromSyntacticNGramStr(syntacticNGramArr);
		String path = pathFromSyntacticNGramStr(syntacticNGramArr);
		
		if (slotX == null || slotY == null || path == null || count == 0) {
			L.log("Failed to parse, one of the objects was not proper: " + ngramStr);
			return null;
		}
		
		NGram ngram = new NGram(path, slotX, slotY, count);
		
		return ngram;
		
	}
	
	private static String slotYFromSyntacticNGramStr(String[] syntacticNGramArr) {
		if (syntacticNGramArr == null || syntacticNGramArr.length == 0) {
			L.log("Cannot fetch slotY from empty array.");
			return null;
		}
		
		// TODO: fetch real slotY.
		return syntacticNGramArr[syntacticNGramArr.length-1];
	}
	
	private static String slotXFromSyntacticNGramStr(String[] syntacticNGramArr) {
		if (syntacticNGramArr == null || syntacticNGramArr.length == 0) {
			L.log("Cannot fetch slotX from empty array.");
			return null;
		}
		
		// TODO: fetch real slotX.
		return syntacticNGramArr[0];
	}
	
	private static String pathFromSyntacticNGramStr(String[] syntacticNGramArr) {
		if (syntacticNGramArr == null || syntacticNGramArr.length < 3) {
			L.log("Cannot fetch path from array " + syntacticNGramArr);
			return null;
		}
		
		String acc = "";
		for (int i = 1 ; i < syntacticNGramArr.length-2 ; i++) {
			acc += syntacticNGramArr[i];
		}
		
		return acc;
	}
}
