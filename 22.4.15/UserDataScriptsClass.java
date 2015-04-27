package task1;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;

public class UserDataScriptsClass {
	
	private static final String mAccesKey = "XXXXX";
	private static final String mSecretCccesKey = "XXXX";
	private static final String mPrivateBuckerName = "mevuzarot.task1";
	
	
	public static String getManagerStartupScript(String jarName, String jarMainClass, String parameters){
        ArrayList<String> lines = new ArrayList<String>();
        
        // start of script
        lines.add("#!/bin/sh");
        lines.add("BIN_DIR=/tmp");
        lines.add("mkdir -p $BIN_DIR/dependencies");
        lines.add("cd $BIN_DIR/dependencies");
        lines.add("wget http://mevuzarot.task1.s3.amazonaws.com/jars/joined.zip");
        lines.add("unzip joined.zip");
        lines.add("mkdir -p $BIN_DIR/running");
        lines.add("cd $BIN_DIR/running");
        lines.add("mkdir -p $BIN_DIR/running/task1_lib");
        lines.add("cp ../dependencies/*.jar task1_lib/");
        lines.add("AWS_ACCESS_KEY_ID=" + mAccesKey);
        lines.add("AWS_SECRET_ACCESS_KEY=" + mSecretCccesKey);
        lines.add("AWS_DEFAULT_REGION=us-east-1");
        lines.add("export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION");
        lines.add("aws s3 cp s3://" + mPrivateBuckerName + "/task1.jar task1.jar");
        lines.add("echo accessKey=$AWS_ACCESS_KEY_ID > credentials.file");
        lines.add("echo secretKey=$AWS_SECRET_ACCESS_KEY >> credentials.file");
        lines.add("java -cp task1.jar " + jarMainClass + " " + parameters);
        // end of script
        String str = new String(Base64.encodeBase64(join(lines, "\n").getBytes()));
        return str;
    }

    private static String join(Collection<String> s, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append(delimiter);
        }
        return builder.toString();
    }

}
