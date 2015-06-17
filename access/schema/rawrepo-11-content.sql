

UPDATE queuerules SET mimetype='' WHERE provider = 'opencataloging-update' AND worker = 'basis-decentral';
UPDATE records SET mimetype='text/marcxchange' WHERE mimetype='text/decentral+marcxchange';



