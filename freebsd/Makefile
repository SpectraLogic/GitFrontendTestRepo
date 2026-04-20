MK_MAN=		no

PREFIX?=	/usr/local
SCRIPTS=	dataplanner tapebackendsim frontend
SCRIPTSDIR?=	${PREFIX}/etc/rc.d

CLEANFILES+=	dataplanner tapebackendsim frontend

dataplanner: rc.dataplanner.in
	sed -e 's,/usr/local,${PREFIX},g' ${.ALLSRC} > ${.TARGET}

tapebackendsim: rc.tapebackendsim.in
	sed -e 's,/usr/local,${PREFIX},g' ${.ALLSRC} > ${.TARGET}

frontend: rc.frontend.in
	sed -e 's,/usr/local,${PREFIX},g' ${.ALLSRC} > ${.TARGET}

.include <bsd.prog.mk>
