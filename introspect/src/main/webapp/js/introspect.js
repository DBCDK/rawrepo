
$('document').ready(function () {

    /**
     * Fallback for console.log
     * 
     * @returns {undefined}
     */
    var LOG = function () {
    };
    if (console && console.log)
        LOG = console.log;

    var ERROR = function (text) {
        var d = $('<div class="ui-button ui-widget ui-corner-all ui-state-default"/>');
        d.text(text);
        $('#errors').append(d);
        setTimeout(function () {
            d.remove();
        }, 3000);
    };



    /**
     * Ajax error function
     * 
     * @param {type} jqXHR
     * @param {type} textStatus
     * @param {type} errorThrown
     * @returns {undefined}
     */
    function ajaxError(jqXHR, textStatus, errorThrown) {
        LOG(textStatus);
        LOG(errorThrown);
        ERROR(errorThrown);
    }

    /**
     * Create key(string) from composite
     * 
     * @param {type} id
     * @returns {String}
     */
    function recordKey(id) {
        return id.agencyid + "," + id.bibliographicrecordid;
    }


    /**
     * Relations object
     * 
     * @param {SVGRelations} svgRelations
     * @returns {RelationCollection}
     */
    var RelationCollection = function (svgRelations) {
        this.svgRelations = svgRelations;
        this.weight = 0;
        this.collection = {};
        this.nodes = [];
        this.links = [];
        this.svgRelations.setCollection(this);
        return this;
    };

    /**
     * Ensure all nodes exist (maby with empty content)
     * 
     * @param {list of ids} rels
     * @returns {undefined}
     */
    RelationCollection.prototype.ensureRelations = function (rels) {
        rels.forEach(function (e) {
            var key = recordKey(e);
            e.key = key;
            if (!(key in this.collection)) {
                var d = {key: key, content: null, relations: {}};
                this.collection[key] = d;
                this.nodes.push(d);
            }
        }, this);
    };

    /**
     * Ensure that a link exist
     * 
     * @param {string} source
     * @param {string} target
     * @param {string} type
     * @returns {undefined}
     */
    RelationCollection.prototype.ensureLink = function (source, target, type) {
        if (!(target in this.collection[source].relations)) {
            this.collection[source].relations[target] = true;
            this.links.push({source: this.collection[source], target: this.collection[target], type: type});
        }
    };

    /**
     * Add a record to the collection
     * 
     * @param {type} record
     */
    RelationCollection.prototype.add = function (record) {
        var key = recordKey(record['me']);
        record['me'].key = key;
        record.weight = (
                Object.keys(record['parents']).length +
                Object.keys(record['children']).length +
                Object.keys(record['siblings-in']).length +
                Object.keys(record['siblings-out']).length
                );
        if (!(key in this.collection)) {
            var d = {key: key, content: record, relations: {}};
            this.collection[key] = d;
            this.nodes.push(d);
        } else if (this.collection[key].content === null) {
            this.collection[key].content = record;
        }

        this.ensureRelations(record['parents']);
        this.ensureRelations(record['children']);
        this.ensureRelations(record['siblings-out']);
        this.ensureRelations(record['siblings-in']);
        record['parents'].forEach(function (e) {
            this.ensureLink(key, e.key, 'parent');
        }, this);
        record['children'].forEach(function (e) {
            this.ensureLink(e.key, key, 'parent');
        }, this);
        record['siblings-out'].forEach(function (e) {
            this.ensureLink(key, e.key, 'sibling');
        }, this);
        record['siblings-in'].forEach(function (e) {
            this.ensureLink(e.key, key, 'sibling');
        }, this);
        this.svgRelations.refresh();
    };

    /**
     * Load a new records relations
     * 
     * @param {int} agencyid
     * @param {string} bibliographicrecordid
     */
    RelationCollection.prototype.load = function (agencyid, bibliographicrecordid) {
        var key = agencyid + "," + bibliographicrecordid;
        if (key in this.collection && this.collection[key].content !== null)
            return;
        var that = this;
        $.ajax({
            type: "GET",
            dataType: "json",
            url: "resources/relations/" + agencyid + "/" + bibliographicrecordid,
            success: function (data, textStatus, jqXHR) {
                that.add(data);
            },
            error: ajaxError
        });
    };

    /*
     * RELATIONS D3JS RELATED OBJECTS
     */
    var svgRelations = new SVGRelations('#svg-container');
    var currentRelations = new RelationCollection(svgRelations);
    $(window).resize(svgRelations.resize.bind(svgRelations));

    /**
     * Callback for bibliographicrecordid selectmenu
     * 
     * @param {type} nextagencyid if an agencyid has been selected
     * @returns {undefined}
     */
    function bibliographicRecordIdSelected(nextagencyid) {
        clear('agencyid');
        var bibliographicrecordid = $('#bibliographicrecordid').val();
        if (bibliographicrecordid === '')
            return;
        $.ajax({
            type: "GET",
            dataType: "json",
            url: "resources/libraries-with/" + bibliographicrecordid,
            success: function (data, textStatus, jqXHR) {
                fillAgencies(data, nextagencyid);
            },
            error: ajaxError
        });
    }

    /**
     * callback for ajax, when bib-id has been selected
     * 
     * Fills agencyid selectmenu
     * 
     * @param {type} data list of agencies
     * @param {type} nextagencyid agencyid which is selected
     * @returns {undefined}
     */
    function fillAgencies(data, nextagencyid) {
        data.forEach(function (e) {
            var option = $('<option/>');
            option.attr({value: e}).text(e);
            $('#agencyid').append(option);
        });
        if (nextagencyid !== null) {
            var i = $('#agencyid option[value="' + nextagencyid + '"]').index();
            if (i > 0) {
                $('#agencyid').prop('selectedIndex', i);
                agencyIdSelected();
            }
        }
        $('#agencyid').selectmenu('refresh');
    }

    /**
     * callback for agencyid selectmenu
     * 
     * @returns {undefined}
     */
    function agencyIdSelected() {
        clear('version');
        var bibliographicrecordid = $('#bibliographicrecordid').val();
        if (bibliographicrecordid === '')
            return;
        var agencyid = $('#agencyid').val();
        if (agencyid === '')
            return;
        $.ajax({
            type: "GET",
            dataType: "json",
            url: "resources/record-history/" + agencyid + "/" + bibliographicrecordid,
            success: function (data, textStatus, jqXHR) {
                fillVersion(data);
            },
            error: ajaxError
        });
        $.ajax({
            type: "GET",
            dataType: "json",
            url: "resources/relations/" + agencyid + "/" + bibliographicrecordid,
            success: function (data, textStatus, jqXHR) {
                fillRelations(data);
            },
            error: ajaxError
        });
    }


    /**
     * callback for ajaf, when agencyid has been selected
     * 
     * fill version selectmenus
     * 
     * @param {type} data list of versions
     * @returns {undefined}
     */
    function fillVersion(data) {
        clear('version');
        data.forEach(function (e, i) {
            var option = $('<option/>');
            option.attr({value: i}).text(e['modified']);
            $('#version').append(option);
        });
        $('#version').selectmenu('refresh');
        data.forEach(function (e, i) {
            var option = $('<option/>');
            option.attr({value: i}).text(e['modified']);
            $('#other-version').append(option);
        });
        $('#other-version').selectmenu('refresh');
    }

    /**
     * callback for ajaf, when agencyid has been selected
     *
     * add merge to versions if needed
     * 
     * setup d3js of relations
     * 
     * @param {type} data
     * @returns {undefined}
     */
    function fillRelations(data) {
        if (data['siblings-out'].length > 0) {
            $('#version option:first-child').first().after($('<option value="-1">Merge</option>'));
            $('#version').selectmenu('refresh');
        }
        currentRelations = new RelationCollection(svgRelations);
        currentRelations.add(data);
    }

    /**
     * callback for version(s) selectmenu
     * 
     * @returns {undefined}
     */
    function versionSelected() {
        clear('content');
        $('#other-version').selectmenu('disable');
        var bibliographicrecordid = $('#bibliographicrecordid').val();
        if (bibliographicrecordid === '')
            return;
        var agencyid = $('#agencyid').val();
        if (agencyid === '')
            return;
        var version = $('#version').val();
        var otherVersion = $('#other-version').val();
        if (version === '')
            return;
        if (version === '-1') {
            $.ajax({
                type: "GET",
                dataType: "json",
                url: "resources/record-merged/" + agencyid + "/" + bibliographicrecordid,
                success: function (data, textStatus, jqXHR) {
                    showRecord(data);
                },
                error: ajaxError
            });
        } else {
            $('#other-version').selectmenu('enable');
            if (otherVersion === '') {
                $.ajax({
                    type: "GET",
                    dataType: "json",
                    url: "resources/record-historic/" + agencyid + "/" + bibliographicrecordid + "/" + version,
                    success: function (data, textStatus, jqXHR) {
                        showRecord(data);
                    },
                    error: ajaxError
                });

            } else {
                $.ajax({
                    type: "GET",
                    dataType: "json",
                    url: "resources/record-diff/" + agencyid + "/" + bibliographicrecordid + "/" + version + "/" + otherVersion,
                    success: function (data, textStatus, jqXHR) {
                        showDiff(data);
                    },
                    error: ajaxError
                });
            }
        }
    }

    /**
     * callback for ajax, record fetched
     * 
     * @param {type} data
     * @returns {undefined}
     */
    function showRecord(data) {
        var content = $("<div>").val(data['content']).format({method: 'xml'}).val();
        $('#content').text(content);
    }

    /**
     * callback for ajax, record diff fetched
     * 
     * @param {type} list
     * @returns {undefined}
     */
    function showDiff(list) {
        var content = $('#content');
        list.forEach(function (e) {
            var f = $('<span class="' + e.type + '"/>');
            f.text(e.content);
            content.append(f);
        });
    }


    /**
     * Ui cleanup
     * 
     * @param {type} from
     * @returns {undefined}
     */
    function clear(from) {
        var c = false;
        c = (c || from === 'agencyid');
        if (c) {
            $('#agencyid').children().each(function (i) {
                if (i > 0)
                    $(this).remove();
            });
            $('#agencyid').selectmenu('refresh');
        }
        c = (c || from === 'version');
        if (c) {
            $('#version').children().each(function (i) {
                if (i > 0)
                    $(this).remove();
            });
            $('#version').selectmenu('refresh');
            $('#other-version').children().each(function (i) {
                if (i > 0)
                    $(this).remove();
            });
            $('#other-version').selectmenu('refresh');
        }
        c = (c || from === 'content');
        if (c) {
            $('#content').contents().remove();
        }
    }


    /*
     * SETUP WIDGETS
     */
    $("#recordTabs").tabs({
        beforeActivate: function (event, ui) {
            if (ui.oldPanel.find('svg').length > 0) {
                svgRelations.stop();
            }
        },
        activate: function (event, ui) {
            ui.newPanel.children('select').selectmenu('refresh');
            if (ui.newPanel.find('svg').length > 0) {
                svgRelations.resize();
                svgRelations.start();
            }
        }
    });
    $('#bibliographicrecordid').button();
    $('#agencyid').selectmenu({
        select: function (event, ui) {
            agencyIdSelected();
        }
    });
    $('#version').selectmenu({
        select: function (event, ui) {
            versionSelected();
        }
    });
    $('#other-version').selectmenu({
        select: function (event, ui) {
            versionSelected();
        }
    });
    $('#bibliographicrecordid').keyup(function (e) {
        if (e.keyCode === 13)
            $(this).trigger("enterKey");
    });
    $('#bibliographicrecordid').bind("enterKey", function () {
        bibliographicRecordIdSelected();
    });
    $('#bibliographicrecordid').bind("focusout", function () {
        bibliographicRecordIdSelected();
    });
    $('#bibliographicrecordid').focus();
    $('#agencyid').bind('change', function () {
        agencyIdSelected();
    });
    $('#version').bind('change', function () {
        versionSelected();
    });
    svgRelations.setOnHover(function (key) {
        if (key === null)
            key = '';
        $('#record-hover').val(key);
    });
    svgRelations.setOnSelect(function (key) {
        if (key === null)
            key = '';
        $('#record-select').val(key);
    });
    svgRelations.setOnClick(function (agencyid, bibliograohicrecordid) {
        $('#bibliographicrecordid').val(bibliograohicrecordid);
        bibliographicRecordIdSelected(agencyid);
    });
    $('#svg-container').resizable();
});
