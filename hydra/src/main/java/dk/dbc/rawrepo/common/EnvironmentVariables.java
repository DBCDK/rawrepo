/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.common;

import javax.ejb.Stateless;
import java.util.Map;

/*
    The purpose of this class is to make mocking of System.getenv easier.
 */
@Stateless
public class EnvironmentVariables {

    public String getenv(String key) {
        return System.getenv(key);
    }

    public Map<String, String> getenv() {
        return System.getenv();
    }
}
