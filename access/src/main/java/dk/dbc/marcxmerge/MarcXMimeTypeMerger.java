package dk.dbc.marcxmerge;

public class MarcXMimeTypeMerger {

    public static boolean canMerge(String originalMimeType, String enrichmentMimeType) {
        switch (originalMimeType) {
            case MarcXChangeMimeType.MARCXCHANGE:
                if (MarcXChangeMimeType.ENRICHMENT.equals(enrichmentMimeType)) {
                    return true;
                }

            case MarcXChangeMimeType.ARTICLE:
                if (MarcXChangeMimeType.ENRICHMENT.equals(enrichmentMimeType)) {
                    return true;
                }

            case MarcXChangeMimeType.AUTHORITY:
                if (MarcXChangeMimeType.ENRICHMENT.equals(enrichmentMimeType)) {
                    return true;
                }

            case MarcXChangeMimeType.LITANALYSIS:
                if (MarcXChangeMimeType.ENRICHMENT.equals(enrichmentMimeType)) {
                    return true;
                }
            case MarcXChangeMimeType.MATVURD:
                if (MarcXChangeMimeType.ENRICHMENT.equals(enrichmentMimeType)) {
                    return true;
                }
            case MarcXChangeMimeType.HOSTPUB:
                if (MarcXChangeMimeType.ENRICHMENT.equals(enrichmentMimeType)) {
                    return true;
                }
            case MarcXChangeMimeType.SIMPLE:
                if (MarcXChangeMimeType.ENRICHMENT.equals(enrichmentMimeType)) {
                    return true;
                }
        }
        return false;
    }

    public static String mergedMimetype(String originalMimeType, String enrichmentMimeType) {
        switch (originalMimeType) {
            case MarcXChangeMimeType.MARCXCHANGE:
                if (MarcXChangeMimeType.ENRICHMENT.equals(enrichmentMimeType)) {
                    return MarcXChangeMimeType.MARCXCHANGE;
                }

            case MarcXChangeMimeType.ARTICLE:
                if (MarcXChangeMimeType.ENRICHMENT.equals(enrichmentMimeType)) {
                    return MarcXChangeMimeType.ARTICLE;
                }

            case MarcXChangeMimeType.AUTHORITY:
                if (MarcXChangeMimeType.ENRICHMENT.equals(enrichmentMimeType)) {
                    return MarcXChangeMimeType.AUTHORITY;
                }

            case MarcXChangeMimeType.LITANALYSIS:
                if (MarcXChangeMimeType.ENRICHMENT.equals(enrichmentMimeType)) {
                    return MarcXChangeMimeType.LITANALYSIS;
                }

            case MarcXChangeMimeType.MATVURD:
                if (MarcXChangeMimeType.ENRICHMENT.equals(enrichmentMimeType)) {
                    return MarcXChangeMimeType.MATVURD;
                }

            case MarcXChangeMimeType.HOSTPUB:
                if (MarcXChangeMimeType.ENRICHMENT.equals(enrichmentMimeType)) {
                    return MarcXChangeMimeType.HOSTPUB;
                }

            case MarcXChangeMimeType.SIMPLE:
                if (MarcXChangeMimeType.ENRICHMENT.equals(enrichmentMimeType)) {
                    return MarcXChangeMimeType.SIMPLE;
                }
        }
        throw new IllegalStateException("Cannot figure out mimetype of: " + originalMimeType + "&" + enrichmentMimeType);
    }
}
