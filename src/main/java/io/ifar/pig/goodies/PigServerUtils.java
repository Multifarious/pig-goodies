package io.ifar.pig.goodies;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class PigServerUtils {


    /**
     * @param script a Pig script to be run
     * @param udfPackages a list of UDF packages
     * @param params a map of parameters to pass to the Pig script
     * @return the
     * @throws IOException
     * @throws ExecException
     */
    public static PigServer prepPigServer(InputStream script, List<String> udfPackages, Map<String,String> params)
            throws IOException, ExecException
    {
        for (String udfPackage : udfPackages) {
            if (!udfPackage.endsWith(".")) {
                udfPackage = udfPackage + ".";
            }
            if (!PigContext.getPackageImportList().contains(udfPackage)) {
                PigContext.getPackageImportList().add(udfPackage);
            }
        }

        PigServer pig =  new PigServer(ExecType.LOCAL);
        pig.registerScript(script,params);
        return pig;
    }
}
