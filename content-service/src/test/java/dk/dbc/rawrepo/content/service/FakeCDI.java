package dk.dbc.rawrepo.content.service;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Stateful;
import javax.ejb.Stateless;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
class FakeCDI {

    private static final Logger log = LoggerFactory.getLogger(FakeCDI.class);

    private final HashMap<String, Object> resources = new HashMap<>();
    private final HashMap<String, Object> injections = new HashMap<>();

    public FakeCDI resource(String name, Object value) {
        resources.put(name, value);
        return this;
    }

    public FakeCDI inject(String name, Object value) {
        injections.put(name, value);
        return this;
    }

    public <T> T build(Class<T> clazz) {
        try {
            log.trace("Constructing " + clazz.getCanonicalName());
            Constructor<T> constructor = clazz.getConstructor();
            if (constructor == null) {
                throw new RuntimeException("Class " + clazz.getCanonicalName() + " doesn't have a default constructor in FakeCDI");
            }
            T instance = constructor.newInstance();
            Annotation[] annotations = clazz.getDeclaredAnnotations();
            for (Annotation annotation : annotations) {
                Class<? extends Annotation> annotationType = annotation.annotationType();
                boolean ejb = Stateless.class.isAssignableFrom(annotationType)
                              || Stateful.class.isAssignableFrom(annotationType)
                              || Singleton.class.isAssignableFrom(annotationType)
                              || javax.inject.Singleton.class.isAssignableFrom(annotationType);
                if (ejb) {
                    Field[] fields = clazz.getDeclaredFields();
                    for (Field field : fields) {
                        setField(instance, field);
                    }
                    Method[] methods = clazz.getDeclaredMethods();
                    for (Method method : methods) {
                        if (method.getGenericParameterTypes().length == 0 && method.getAnnotation(PostConstruct.class) != null) {
                            System.out.println("Calling @PostConstruct " + method.getName());
                            method.invoke(instance);
                        }
                    }
                    break;
                }
            }
            return instance;
        } catch (NoSuchMethodException | SecurityException |
                 InstantiationException | IllegalAccessException |
                 IllegalArgumentException | InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }
    }

    private <T> void setField(T instance, Field field) throws IllegalArgumentException, IllegalAccessException {
        if (injections.containsKey(field.getName())) {
            log.trace("Setting " + field.getName());
            field.set(instance, injections.get(field.getName()));
            return;
        }
        Resource resource = field.getAnnotation(Resource.class);
        if (resource != null) {
            String name = resource.name() + resource.mappedName() + resource.lookup();
            log.trace("@Resource " + field.getName() + " name=" + name + "; resource=" + resources.get(name));
            field.set(instance, resources.get(name));
            return;
        }
        Inject inject = field.getAnnotation(Inject.class);
        if (inject != null) {
            log.trace("@Inject " + field.getName());
            field.set(instance, build(field.getType()));
            return;
        }
        EJB ejb = field.getAnnotation(EJB.class);
        if (ejb != null) {
            log.trace("@EJB " + field.getName());
            field.set(instance, build(field.getType()));
            return;
        }
    }

}
