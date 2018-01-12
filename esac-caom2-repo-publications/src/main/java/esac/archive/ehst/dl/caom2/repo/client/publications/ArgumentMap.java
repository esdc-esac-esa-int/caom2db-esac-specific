package esac.archive.ehst.dl.caom2.repo.client.publications;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Simple command-line argument utility that takes all arguments of the form --key=value and stores them in a map of key=value. As a shortcut, --key is
 * equivalent to --key=<em>true</em>, where <em>true</em> is the String representation of Boolean.TRUE. Arguments that start with a single dash (-) are always
 * mapped to Boolean.TRUE, whether or not they contain an = sign.
 *
 * As an added bonus/complexity, the character sequence %% can be used to delimit values that would otherwise be split up by the invoking shell.
 *
 * @version $Revision: 325 $
 * @author $Author: pdowler $
 */
public class ArgumentMap {
    @SuppressWarnings("unchecked")
    private Map<String, Object> map = new HashMap<>();
    private List<String> pos = new ArrayList<>();

    public ArgumentMap(String[] args) {
        this.map = new HashMap();
        for (int i = 0; i < args.length; i++) {
            String key = null;
            String str = null;
            Object value = null;
            if (args[i].startsWith("--")) {
                // put generic arg in argmap
                try {
                    int j = args[i].indexOf('=');
                    if (j <= 0) {
                        // map to true
                        key = args[i].substring(2, args[i].length());
                        value = Boolean.TRUE;
                    } else {
                        // map to string value
                        key = args[i].substring(2, j);
                        str = args[i].substring(j + 1, args[i].length());

                        // special %% stuff %% delimiters
                        if (str.startsWith("%%")) {
                            // look for the next %% on the command-line
                            str = str.substring(2, str.length());
                            if (str.endsWith("%%")) {
                                value = str.substring(0, str.length() - 2);
                            } else {
                                StringBuilder sb = new StringBuilder(str);
                                boolean done = false;
                                while (i + 1 < args.length && !done) {
                                    i++;
                                    if (args[i].endsWith("%%")) {
                                        str = args[i].substring(0, args[i].length() - 2);
                                        done = true;
                                    } else
                                        str = args[i];
                                    sb.append(" " + str);
                                }
                                value = sb.toString();
                            }
                        } else
                            value = str;
                    }
                } catch (Exception ignorable) {
                    //log.debug(" skipping: " + ignorable.toString());
                }
            } else if (args[i].startsWith("-")) {
                try {
                    key = args[i].substring(1, args[i].length());
                    value = Boolean.TRUE;
                } catch (Exception ignorable) {
                    //log.debug(" skipping: " + ignorable.toString());
                }
            } else {
                pos.add(args[i]);
                //log.debug("pos: " + args[i]);
            }
            if (key != null && value != null) {
                //log.debug("map: " + key + "->" + value);
                Object old_value = map.put(key, value);
                //if (old_value != null) log.debug(" (old mapping removed: " + key + " : " + old_value + ")");
            }
        }
    }

    public String getValue(String key) {
        Object obj = map.get(key);
        if (obj != null)
            return obj.toString();
        return null;
    }

    public List<String> getPositionalArgs() {
        return pos;
    }

    public boolean isSet(String key) {
        return map.containsKey(key);
    }

    @SuppressWarnings("unchecked")
    public Set keySet() {
        return map.keySet();
    }
}
