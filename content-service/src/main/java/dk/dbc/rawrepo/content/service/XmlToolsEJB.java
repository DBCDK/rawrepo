package dk.dbc.rawrepo.content.service;

import javax.inject.Singleton;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@Singleton
public class XmlToolsEJB extends Pool<XmlTools>{

    @Override
    public XmlTools create() {
        return new XmlTools();
    }

}
