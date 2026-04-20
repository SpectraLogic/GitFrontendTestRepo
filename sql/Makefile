.PATH: ${.CURDIR}

PREFIX?=	/usr/local
SQLDIR?=	${PREFIX}/bluestorm/db

MIGRATIONS!=	find ./db/migrate -type f

install:
	mkdir -p ${DESTDIR}${SQLDIR}
	${INSTALL} -m ${CONFMODE} Rakefile ${DESTDIR}${SQLDIR}
	mkdir -p ${DESTDIR}${SQLDIR}/config
	${INSTALL} -m ${CONFMODE} config/database.yml ${DESTDIR}${SQLDIR}/config
	mkdir -p ${DESTDIR}${SQLDIR}/db/migrate
	${INSTALL} -m ${CONFMODE} ${MIGRATIONS} ${DESTDIR}${SQLDIR}/db/migrate

.include <bsd.prog.mk>
