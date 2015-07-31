package model;

import Utils.Constants;
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
	final static int NUMER_OF_OBJECTS_IN_MAIN_ARRAY 	= 3;
	final static int TOTAL_COUNT_INDEX					= 1;
	final static int NGRAM_INDEX 						= 0;
	final static int MIN_SYNTACTIC_NGRAM_LENGTH 		= 3;
	final static DLogger L 								= new DLogger(true);
	final static String SYNTACTIC_NGRAM_HEAD_SPLIT_CHAR = "  "; // Two spaces.
	final static String PARSED_WORD_SPLIT_CHAR 			= "/";
	final static String NOUN_PREFIX 					= "NN";
	final static String VERB_PREFIX 					= "VB";
	final static int INDEX_OF_HEAD_WORD					= 0;
		
	public static NGram[] parseNGram(String ngramStr) {
		
		if (ngramStr == null) {
			L.log("Cannot parse null string.");
			return null;
		}
		
		String[] mainArr = ngramStr.split("      ");
		
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
		String[] syntacticNGramWithHeadArr = syntacticNgram.split(SYNTACTIC_NGRAM_HEAD_SPLIT_CHAR);
		
		if (syntacticNGramWithHeadArr.length != 2) {
			L.log("Not enough args with head in ngram " + syntacticNGramWithHeadArr);
			return null;
		}
		
		syntacticNgram = syntacticNGramWithHeadArr[1];
		String[] syntacticNGramArr = syntacticNgram.split(" ");
		
		
		if (syntacticNGramArr.length < MIN_SYNTACTIC_NGRAM_LENGTH) {
			L.log("Too few arguments in syntactic ngram: ." + syntacticNgram);
			return null;
		}
		
		String slotX = null;
		String slotY = null;
		String path = null;
		
		for (int i = 0 ; i < syntacticNGramArr.length ; i++) {
			String parsedWord =  syntacticNGramArr[i];
			String[] parts = parsedWord.trim().split(PARSED_WORD_SPLIT_CHAR);
			if (parts.length < 4) {
				continue;
			}
			String word = parts[0];
			String type = parts[1];
			int index = -1;
			try {
				index = Integer.parseInt(parts[3]);
			}catch(Exception e) {L.log("Failed to parse word " + parsedWord);}
			
			if (index == -1) {
				continue;
			}
			// Root
			else if (index == 0) {
				if (!type.startsWith(VERB_PREFIX)) {
					// All roots must be verbs!
					return null;
				}
			}
			
			boolean isNoun = type.startsWith(NOUN_PREFIX);
			if (slotX == null && isNoun) {
				slotX = word;
				continue;
			}
			else if (slotY == null && isNoun) {
				slotY = word;
				continue;
			}
			else if (path != null) {
				path += " ";
			}
			
			path += word;
		}
		
		if (slotX == null || slotY == null || path == null || count == 0) {
			L.log("Failed to parse, one of the objects was not proper: " + ngramStr);
			return null;
		}
		
		NGram ngram1 = new NGram(Constants.SLOT_X + Constants.SPACE + path + Constants.SPACE + Constants.SLOT_Y, slotX, slotY, count);
		NGram ngram2 = new NGram(Constants.SLOT_Y + Constants.SPACE + path + Constants.SPACE + Constants.SLOT_X, slotY, slotX, count);
		NGram[] result = new NGram[2];
		result[0] = ngram1;
		result[1] = ngram2;
		
		return result;
	}
	
	private static boolean isWordVerb(String parsedWord) {
		String type = NGramFactory.wordTypeFromString(parsedWord);
		return type.startsWith(VERB_PREFIX);
	}
	
	private static boolean isWordNoun(String parsedWord) {
		String type = NGramFactory.wordTypeFromString(parsedWord);
		return type.startsWith(NOUN_PREFIX);
	}
	
	private static String wordTypeFromString(String parsedWord) {
		String[] parts = parsedWord.trim().split(PARSED_WORD_SPLIT_CHAR);
		if (parts.length < 4) {
			return "";
		}
		
		return parts[1]; 
	}
	
	private static String slotYFromSyntacticNGramStr(String[] syntacticNGramArr) {
		if (syntacticNGramArr == null || syntacticNGramArr.length == 0) {
			L.log("Cannot fetch slotY from empty array.");
			return null;
		}
		
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
		for (int i = 1 ; i < syntacticNGramArr.length-1 ; i++) {
			acc += syntacticNGramArr[i] + " ";
		}
		
		return acc;
	}
}
