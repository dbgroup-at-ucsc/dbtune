/****************************************************************************
 * Copyright 2010 Huascar A. Sanchez                                        *
 *                                                                          *
 * Licensed under the Apache License, Version 2.0 (the "License");          *
 * you may not use this file except in compliance with the License.         *
 * You may obtain a copy of the License at                                  *
 *                                                                          *
 *     http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                          *
 * Unless required by applicable law or agreed to in writing, software      *
 * distributed under the License is distributed on an "AS IS" BASIS,        *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 * See the License for the specific language governing permissions and      *
 * limitations under the License.                                           *
 ****************************************************************************/
package edu.ucsc.satuning.console;

import edu.ucsc.satuning.console.commands.MkDir;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * A representation of a previous invocation.
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Tag {
    private final static String ARGS_DIR = "args";
    // maps ignored command line options (for the purposes of saving the command to a tag) to
    // the number of arguments they take.
    private static Map<String, Integer> optionsToIgnore = new HashMap<String, Integer>();

    private final File argsDir;
    private final String tag;
    private final boolean tagOverwrite;

    public Tag(File tagDir, String tag, boolean tagOverwrite) {
        this.tag = tag;
        this.argsDir = new File(tagDir, ARGS_DIR);
        this.tagOverwrite = tagOverwrite;
    }

    public static String[] getAllTags(File tagDir) {
        return new File(tagDir, ARGS_DIR).list();
    }

    public String[] getArgs() throws FileNotFoundException {
        List<String> args = new ArrayList<String>();
        Scanner scanner = new Scanner(new File(argsDir, tag));
        while (scanner.hasNextLine()) {
            args.add(scanner.nextLine());
        }
        scanner.close();
        return args.toArray(new String[args.size()]);
    }

    public void saveArgs(String[] args) {
        File argsFile = new File(argsDir, tag);
        if (!tagOverwrite && argsFile.exists()) {
            throw new RuntimeException("Tag \"" + tag + "\" already exists, add "
                    + "\"--tag-overwrite\" to override");
        }

        new MkDir().mkdirs(argsFile.getParentFile());

        BufferedWriter tagWriter;
        try {
            tagWriter = new BufferedWriter(new FileWriter(argsFile));
            for (int i = 0; i < args.length; i++) {
                if (optionsToIgnore.containsKey(args[i])) {
                    // skip the arguments to the flag as well
                    i += optionsToIgnore.get(args[i]);
                } else {
                    tagWriter.write(args[i]);
                    tagWriter.newLine();
                }
            }
            tagWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Marks option so it is not saved to a tag file.
     */
    public static void addUnsavedOption(String arg, int numArgs) {
        optionsToIgnore.put(arg, numArgs);
    }

}
