package task1;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;

public class UserDataScriptsClass {
	
	private static final String mAccesKey = "XXXXXX";
	private static final String mSecretCccesKey = "XXXXXX";
	private static final String mPrivateBuckerName = "mevuzarot.task1";
	
	
	public static String getManagerStartupScript(String jarName, String jarMainClass, String parameters){
        ArrayList<String> lines = new ArrayList<String>();
        
        // start of script
        lines.add("#!/bin/sh");
        lines.add("BIN_DIR=/tmp");
        lines.add("AWS_ACCESS_KEY_ID=" + mAccesKey);
        lines.add("AWS_SECRET_ACCESS_KEY=" + mSecretCccesKey);
        lines.add("AWS_DEFAULT_REGION=us-east-1");
        lines.add("# Make depenedencies");
        lines.add("mkdir -p $BIN_DIR/dependencies");
        lines.add("cd $BIN_DIR/dependencies");
        lines.add("wget http://sdk-for-java.amazonwebservices.com/latest/aws-java-sdk.zip");
        lines.add("unzip aws-java-sdk.zip");
        lines.add("mv aws-java-sdk-*/ aws-java-sdk");
        lines.add("wget http://www.us.apache.org/dist//commons/io/binaries/commons-io-2.4-bin.zip");
        lines.add("unzip commons-io-2.4-bin.zip");
        lines.add("# Make main app");
        lines.add("cd $BIN_DIR");
        lines.add("mkdir -p $BIN_DIR/bin/jar");
        lines.add("export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION");
        lines.add("aws s3 cp s3://" + mPrivateBuckerName + "/" + jarName + " $BIN_DIR/bin/jar");
        lines.add("echo accessKey=$AWS_ACCESS_KEY_ID > $BIN_DIR/credentials.csv");
        lines.add("echo secretKey=$AWS_SECRET_ACCESS_KEY >> $BIN_DIR/credentials.csv");
        lines.add("# Java would compile the following line:");
        lines.add("java -cp $BIN_DIR/bin/jar/" + jarName + " " + jarMainClass + " " + parameters);
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
