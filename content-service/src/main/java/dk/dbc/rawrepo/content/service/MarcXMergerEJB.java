package dk.dbc.rawrepo.content.service;

import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import javax.annotation.PostConstruct;
import javax.ejb.EJBException;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@Singleton
public class MarcXMergerEJB extends Pool<MarcXMerger> {

    private static final Logger log = LoggerFactory.getLogger(MarcXMergerEJB.class);

    @PostConstruct
    public void init() {
        log.warn("init()");
    }

    @Override
    public MarcXMerger create() {
        try {
            return new MarcXMerger();
        } catch (MarcXMergerException ex) {
            throw new EJBException("Cannot init MarcXChangeMerger", ex);
        }
    }

}
