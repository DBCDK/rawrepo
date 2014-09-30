#!/bin/bash -e

cd "${0%/*}"
if [ "${0:0:1}" != '/' ]; then
    exec "`pwd`/${0/*\//}" "$@"
fi

if [ `whoami` != root ]; then
    exec fakeroot "$0" "$@"
fi

cd "${0%/*}"

export PACKAGE=`svn info ../ | awk '{if($1=="URL:")print$2}' | sed 's!.*/\([^/]*\)/\(trunk\|tags/.*\|revisions/.*\)!\1!'`
export REVISION=`svn info ../ | awk '{if($1=="Revision:")print$2}'`
export USER=`env LD_PRELOAD= whoami`
export TIMESTAMP=`date +'%a, %_d %b %Y %_H:%M:%S %z'`
export DIST=`awk '{if($1=="deb"){print$3;exit}}' /etc/apt/sources.list`

rm -rf debian
mkdir -p debian
mkdir -p debian/$PACKAGE-tools-dbc/usr/bin
mkdir -p debian/$PACKAGE-tools-dbc/usr/lib/$PACKAGE

echo 5 > debian/compat

sed -e '1,/^__CONTROL__/d;/^__END__/,$d' -e '/^#/d' $0 | perl -p -e 'BEGIN{%h=map{split"=",$_,2}@ARGV;@ARGV=()}s{\@(\w+)\@}{$h{$1}}ge' \
    PACKAGE="$PACKAGE" \
    REVISION="$BUILD_NUMBER.$REVISION" \
    USER="$USER" \
    TIMESTAMP="$TIMESTAMP" \
    > debian/control

#
# Find revisions to each module
# set r?=1
#
while read module tool; do
    for rev in $(svn log -l 100 "../$module" | (
	    while read dashes; do
		[ "$dashes" != "" ] && [ "${dashes//-/}" = "" ] || exit 0
		IFS="|" read rev user date lines || exit 0
		declare -i i=${lines//[^0-9]/}
		read blank || exit 0
		[ "$blank" == "" ] || exit 0
		while [ $i != 0 ]; do
		    i=$i-1
		    read line || exit 1
		done
		echo $rev
	    done
	    )); do
	eval $rev=1
    done
done <<EOF
$(sed -e '1,/^__LIST__/d' -e '/^\(#.*\)*$/d' $0)
EOF


#
# extract all changelogs regarding these modules
#
svn log -l 100 .. | (
    # Ensure newest revision in changelog
    if eval [ \$\{r$REVISION:-0\} != 1 ]; then
	echo "$PACKAGE-dbc ($BUILD_NUMBER.$REVISION) stable; urgency=low"
	echo ""
	echo "  * Automated build"
	echo ""
	echo " -- Maintainer <${user/@*/}@dbc.dk>  `date -d "$date" +'%a, %_d %b %Y %_H:%M:%S %z'`"
	echo ""
    fi

    while read dashes; do
	if [ "$dashes" = "" ] || [ "${dashes//-/}" != "" ]; then
	    exit 0
	fi
	IFS="|" read rev user date lines || exit 0
	rev="${rev// /}"
	user="${user// /}"
	if eval [ \$\{$rev:-0\} = 1 ]; then
	    echo=echo
	else
	    echo=true
	fi

	$echo "$PACKAGE-dbc ($BUILD_NUMBER.${rev//[^0-9]/}) stable; urgency=low"
	$echo ""
	declare -i i=${lines//[^0-9]/}
	read blank || exit 0
	if [ "$blank" != "" ]; then
	    exit 0
	fi
	prefix="  * "
	while [ $i != 0 ]; do
	    i=$i-1
	    read line || exit 1
	    $echo "$prefix$line"
	    prefix="    "
	done
	$echo ""
	$echo " -- Maintainer <${user/@*/}@dbc.dk>  `date -d "$date" +'%a, %_d %b %Y %_H:%M:%S %z'`"
	$echo ""
    done
) >debian/changelog



#
# build executable & copy jar
#
while read module tool; do
    for t in $tool; do
	(
		echo "#!/bin/sh"
		echo "exec /usr/bin/java -jar /usr/lib/$PACKAGE/$t.jar "'"$@"'
	) > debian/$PACKAGE-tools-dbc/usr/bin/$t
	chmod +x debian/$PACKAGE-tools-dbc/usr/bin/$tool
	cp ../$module/target/$t.jar debian/$PACKAGE-tools-dbc/usr/lib/$PACKAGE
    done
done <<EOF
$(sed -e '1,/^__LIST__/d' -e '/^\(#.*\)*$/d' $0)
EOF

dh_testdir
dh_testroot
dh_installdirs
dh_installchangelogs
dh_compress
dh_fixperms
dh_installdeb
dh_gencontrol
dh_md5sums
dh_builddeb --destdir=.
dpkg-genchanges -b -u. > $PACKAGE-tools-dbc_$BUILD_NUMBER.$REVISION.changes
LD_PRELOAD= rsync $PACKAGE-tools-dbc_$BUILD_NUMBER.$REVISION.changes $PACKAGE-tools-dbc_$BUILD_NUMBER.${REVISION}_all.deb drift@debian.dbc.dk:is/$DIST



exit 0

#
# debian/control file:
#
__CONTROL__
Source: @PACKAGE@-dbc
Priority: extra
Maintainer: <@USER@@dbc.dk>
Build-Depends: debhelper (>= 4), autotools-dev
Standards-Version: @RELEASE@
Section: libs

Package: @PACKAGE@-tools-dbc
Section: utils
Architecture: all
Depends: jdk7-dbc
Description: DBC java tools for @PACKAGE@
 DBC java tools for @PACKAGE@

__END__



__LIST__
# module (to include in changelog) [jar ...]
access
record-inspector rawrepo-record-inspector
record-load rawrepo-record-load
queue-bulkload rawrepo-queue-bulkload
