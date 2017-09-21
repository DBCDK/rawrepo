/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.json;

import java.util.HashMap;

public class QueueWorker {

    private String name;
    private String changed;
    private String leaf;
    private String description;

    public QueueWorker(String name, String changed, String leaf) {
        this.name = name;
        this.changed = changed;
        this.leaf = leaf;

        // This class is used as JSON object which means the description value have to set up front
        generateDescription();
    }

    private void generateDescription() {
        // Stolen from rawrepo-maintain
        HashMap<String, String> descriptions = new HashMap<>();
        descriptions.put("NN", "Hoved/Sektionsposter som er afhængige af den rørte post og ikke er rørt");
        descriptions.put("NY", "Bind/Enkeltstående poster som er afhængige af den rørte post og ikke er rørt");
        descriptions.put("NA", "Alle poster som er afhængige af den rørte post");
        descriptions.put("YN", "Den rørte post, hvis det er en Hoved/Sektionsport");
        descriptions.put("YY", "Den rørte post, hvis det er en Bind/Enkeltstående post");
        descriptions.put("YA", "Den rørte post");
        descriptions.put("AN", "Alle Hoved/Sektionsposter som er afhængige af den rørte post incl den rørte post");
        descriptions.put("AY", "Alle Bind/Enkeltstående poster som er afhængige af den rørte post incl den rørte post");
        descriptions.put("AA", "Den rørte post og alle poster som er afhængige af den");

        this.description = descriptions.get(this.changed.toUpperCase() + this.leaf.toUpperCase());
    }

    // Getters might be used by JSON mapper, so don't remove
    public String getName() {
        return name;
    }

    public String getChanged() {
        return changed;
    }

    public String getLeaf() {
        return leaf;
    }

    public String getDescription() {
        return description;
    }
}
