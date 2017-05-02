/*
 * dbc-rawrepo-maintain
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-maintain.
 *
 * dbc-rawrepo-maintain is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-maintain is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-maintain.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.maintain;

import dk.dbc.rawrepo.QueueTarget;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class QueueRules extends RawRepoWorker {

    public QueueRules(DataSource dataSource, QueueTarget queueTarget) {
        super(dataSource, queueTarget, null, null);
    }

    public ArrayList<Provider> getQueueRules() throws SQLException {

        try (PreparedStatement stmt = getConnection().prepareStatement("SELECT provider,worker,changed,leaf FROM queuerules ORDER BY provider")) {
            try (ResultSet resultSet = stmt.executeQuery()) {

                HashMap<String, Provider> providerMap = new HashMap<>();
                ArrayList<Provider> res = new ArrayList<>();

                while (resultSet.next()) {
                    int i = 0;
                    String provider = resultSet.getString(++i);
                    String worker = resultSet.getString(++i);
                    String changed = resultSet.getString(++i);
                    String leaf = resultSet.getString(++i);

                    Provider p = providerMap.get(provider);
                    if (p == null) {
                        p = new Provider(provider);
                        providerMap.put(provider, p);
                        res.add(p);
                    }

                    p.getWorkers().add(new Worker(worker, changed, leaf));

                }
                return res;
            }
        }
    }

    public static class Provider {

        private final String provider;
        private final List<Worker> workers;

        public Provider(String provider) {
            this.provider = provider;
            workers = new ArrayList<>();
        }

        /**
         * @return the provider
         */
        public String getProvider() {
            return provider;
        }

        /**
         * @return the workers
         */
        public List<Worker> getWorkers() {
            return workers;
        }

    }

    public static class Worker {

        private static final Map<String, String> descriptions;

        static {
            // Key is 'changed+leaf'
            descriptions = new HashMap<>();
            descriptions.put("NN", "Hoved/Sektionsposter som er afhængige af den rørte post og ikke er rørt");
            descriptions.put("NY", "Bind/Enkeltstående poster som er afhængige af den rørte post og ikke er rørt");
            descriptions.put("NA", "Alle poster som er afhængige af den rørte post");
            descriptions.put("YN", "Den rørte post, hvis det er en Hoved/Sektionsport");
            descriptions.put("YY", "Den rørte post, hvis det er en Bind/Enkeltstående post");
            descriptions.put("YA", "Den rørte post");
            descriptions.put("AN", "Alle Hoved/Sektionsposter som er afhængige af den rørte post incl den rørte post");
            descriptions.put("AY", "Alle Bind/Enkeltstående poster som er afhængige af den rørte post incl den rørte post");
            descriptions.put("AA", "Den rørte post og alle poster som er afhængige af den");
        }

        private final String worker, changed, leaf;

        public Worker(String worker, String changed, String leaf) {
            this.worker = worker;
            this.changed = changed;
            this.leaf = leaf;
        }

        /**
         * @return the worker
         */
        public String getWorker() {
            return worker;
        }

        /**
         * @return the changed
         */
        public String getChanged() {
            return changed;
        }

        /**
         * @return the leaf
         */
        public String getLeaf() {
            return leaf;
        }

        public String getDescription() {
            return descriptions.get(this.changed.toUpperCase() + this.leaf.toUpperCase());
        }
    }

}
