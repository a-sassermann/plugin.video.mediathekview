# -*- coding: utf-8 -*-
"""
The MySQL database support module

Copyright 2017-20180 Leo Moll and Dominik Schlösser
"""
# pylint: disable=import-error
# pylint: disable=mixed-indentation, bad-whitespace, bad-continuation, missing-docstring

# -- Imports ------------------------------------------------
import time
import mysql.connector
import hashlib

import resources.lib.mvutils as mvutils

from resources.lib.film import Film

# -- Classes ------------------------------------------------
class StoreMySQL( object ):
	"""The MySQL database support class"""

	def __init__( self, logger, notifier, settings ):
		self.sqlInsert = """ insert into film_import 
				(`idhash`, `channel`, `show`, `showsearch`,
				`title`, `search`, `aired`, `duration`, `size`, `description`, 
				`website`, `url_sub`, `url_video`, `url_video_sd`, `url_video_hd`, 
				`airedepoch`) values 
				"""
		self.sqlValues = ''
		self.sqlData = []
		self.conn		= None
		self.logger		= logger
		self.notifier	= notifier
		self.settings	= settings
		# updater state variables
		self.ft_channel = None
		self.ft_channelid = None
		self.ft_show = None
		self.ft_showid = None
		# useful query fragments
		# pylint: disable=line-too-long
		self.sql_query_films	= "SELECT film.id,`title`,`show`,`channel`,`description`,TIME_TO_SEC(`duration`) AS `seconds`,`size`,`aired`,`url_sub`,`url_video`,`url_video_sd`,`url_video_hd` FROM `film` LEFT JOIN `show` ON `show`.id=film.showid LEFT JOIN `channel` ON channel.id=film.channelid"
		self.sql_query_filmcnt	= "SELECT COUNT(*) FROM `film` LEFT JOIN `show` ON `show`.id=film.showid LEFT JOIN `channel` ON channel.id=film.channelid"
		self.sql_cond_recent	= "( TIMESTAMPDIFF(SECOND,{},CURRENT_TIMESTAMP()) <= {} )".format( "aired" if settings.recentmode == 0 else "film.dtCreated", settings.maxage )
		self.sql_cond_nofuture	= " AND ( ( `aired` IS NULL ) OR ( TIMESTAMPDIFF(HOUR,`aired`,CURRENT_TIMESTAMP()) > 0 ) )" if settings.nofuture else ""
		self.sql_cond_minlength	= " AND ( ( `duration` IS NULL ) OR ( TIME_TO_SEC(`duration`) >= %d ) )" % settings.minlength if settings.minlength > 0 else ""

	def Init( self, reset, convert ):
		self.resetInsertSql()

		self.logger.info( 'Using MySQL connector version {}', mysql.connector.__version__ )
		try:
			self.conn		= mysql.connector.connect(
				host		= self.settings.host,
				port		= self.settings.port,
				user		= self.settings.user,
				password	= self.settings.password
			)
			try:
				cursor = self.conn.cursor()
				cursor.execute( 'SELECT VERSION()' )
				( version, ) = cursor.fetchone()
				self.logger.info( 'Connected to server {} running {}', self.settings.host, version )
			# pylint: disable=broad-except
			except Exception:
				self.logger.info( 'Connected to server {}', self.settings.host )
			self.conn.database = self.settings.database
		except mysql.connector.Error as err:
			if err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
				self.logger.info( '=== DATABASE {} DOES NOT EXIST. TRYING TO CREATE IT ===', self.settings.database )
				return self._handle_database_initialization()
			self.conn = None
			self.logger.error( 'Database error: {}, {}', err.errno, err )
			self.notifier.ShowDatabaseError( err )
			return False

		# handle schema versioning
		return self._handle_database_update( convert )

	def Exit( self ):
		self.logger.info('in Exit')
		if self.conn is not None:
			self.conn.close()
			self.conn = None

	def resetInsertSql(self):
		self.sqlValues = ''
		self.sqlData = []

	def Search( self, search, filmui, extendedsearch ):
		searchmask = '%' + search.decode('utf-8') + '%'
		searchcond = '( ( `title` LIKE %s ) OR ( `show` LIKE %s ) OR ( `description` LIKE %s ) )' if extendedsearch is True else '( ( `title` LIKE %s ) OR ( `show` LIKE %s ) )'
		searchparm = ( searchmask, searchmask, searchmask ) if extendedsearch is True else ( searchmask, searchmask, )
		return self._Search_Condition( searchcond, searchparm, filmui, True, True, self.settings.maxresults )

	def GetRecents( self, channelid, filmui ):
		if channelid != '0':
			return self._Search_Condition( self.sql_cond_recent + ' AND ( film.channelid=%s )', ( int( channelid ), ), filmui, True, False, 10000 )
		else:
			return self._Search_Condition( self.sql_cond_recent, (), filmui, True, False, 10000 )

	def GetLiveStreams( self, filmui ):
		return self._Search_Condition( '( show.search="LIVESTREAM" )', (), filmui, False, False, 0, False )

	def GetChannels( self, channelui ):
		self._Channels_Condition( None, channelui )

	def GetRecentChannels( self, channelui ):
		self._Channels_Condition( self.sql_cond_recent, channelui )

	def GetInitials( self, channelid, initialui ):
		if self.conn is None:
			return
		try:
			channelid = int( channelid )
			cursor = self.conn.cursor()
			if channelid != 0:
				self.logger.info(
					'MySQL Query: SELECT LEFT(`search`,1) AS letter,COUNT(*) AS `count` FROM `show` WHERE ( `channelid`={} ) GROUP BY LEFT(search,1)',
					channelid
				)
				cursor.execute( """
					SELECT		LEFT(`search`,1)	AS `letter`,
								COUNT(*)			AS `count`
					FROM		`show`
					WHERE		( `channelid`=%s )
					GROUP BY	LEFT(`search`,1)
				""", ( channelid, ) )
			else:
				self.logger.info(
					'MySQL Query: SELECT LEFT(`search`,1) AS letter,COUNT(*) AS `count` FROM `show` GROUP BY LEFT(search,1)'
				)
				cursor.execute( """
					SELECT		LEFT(`search`,1)	AS `letter`,
								COUNT(*)			AS `count`
					FROM		`show`
					GROUP BY	LEFT(`search`,1)
				""" )
			initialui.Begin( channelid )
			for ( initialui.initial, initialui.count ) in cursor:
				initialui.Add()
			initialui.End()
			cursor.close()
		except mysql.connector.Error as err:
			self.logger.error( 'Database error: {}, {}', err.errno, err )
			self.notifier.ShowDatabaseError( err )

	def GetShows( self, channelid, initial, showui ):
		if self.conn is None:
			return
		try:
			channelid = int( channelid )
			cursor = self.conn.cursor()
			if channelid == 0 and self.settings.groupshows:
				cursor.execute( """
					SELECT		GROUP_CONCAT(`show`.id),
								GROUP_CONCAT(`channelid`),
								`show`,
								GROUP_CONCAT(`channel`)
					FROM		`show`
					LEFT JOIN	`channel`
						ON		( channel.id = `show`.channelid )
					WHERE		( `show` LIKE %s )
					GROUP BY	`show`
				""", ( initial + '%', ) )
			elif channelid == 0:
				cursor.execute( """
					SELECT		`show`.id,
								`show`.channelid,
								`show`.show,
								channel.channel
					FROM		`show`
					LEFT JOIN	`channel`
						ON		( channel.id = `show`.channelid )
					WHERE		( `show` LIKE %s )
				""", ( initial + '%', ) )
			elif initial:
				cursor.execute( """
					SELECT		`show`.id,
								`show`.channelid,
								`show`.show,
								channel.channel
					FROM		`show`
					LEFT JOIN	`channel`
						ON		( channel.id = `show`.channelid )
					WHERE		(
									( `channelid` = %s )
									AND
									( `show` LIKE %s )
								)
				""", ( channelid, initial + '%', ) )
			else:
				cursor.execute( """
					SELECT		`show`.id,
								`show`.channelid,
								`show`.show,
								channel.channel
					FROM		`show`
					LEFT JOIN	`channel`
						ON		( channel.id = `show`.channelid )
					WHERE		( `channelid` = %s )
				""", ( channelid, ) )
			showui.Begin( channelid )
			for ( showui.id, showui.channelid, showui.show, showui.channel ) in cursor:
				showui.Add()
			showui.End()
			cursor.close()
		except mysql.connector.Error as err:
			self.logger.error( 'Database error: {}, {}', err.errno, err )
			self.notifier.ShowDatabaseError( err )

	def GetFilms( self, showid, filmui ):
		if self.conn is None:
			return
		if showid.find( ',' ) == -1:
			# only one channel id
			return self._Search_Condition( '( `showid` = %s )', ( int( showid ), ), filmui, False, False, 10000 )
		else:
			# multiple channel ids
			return self._Search_Condition( '( `showid` IN ( {} ) )'.format( showid ), (), filmui, False, True, 10000 )

	def _Channels_Condition( self, condition, channelui):
		if self.conn is None:
			return
		try:
			if condition is None:
				query = 'SELECT `id`,`channel`,0 AS `count` FROM `channel`'
				qtail = ''
			else:
				query = 'SELECT channel.id AS `id`,`channel`,COUNT(*) AS `count` FROM `film` LEFT JOIN `channel` ON channel.id=film.channelid'
				qtail = ' WHERE ' + condition + self.sql_cond_nofuture + self.sql_cond_minlength + ' GROUP BY channel.id'
			self.logger.info( 'MySQL Query: {}', query + qtail )

			cursor = self.conn.cursor()
			cursor.execute( query + qtail )
			channelui.Begin()
			for ( channelui.id, channelui.channel, channelui.count ) in cursor:
				channelui.Add()
			channelui.End()
			cursor.close()
		except mysql.connector.Error as err:
			self.logger.error( 'Database error: {}, {}', err.errno, err )
			self.notifier.ShowDatabaseError( err )

	def _Search_Condition( self, condition, params, filmui, showshows, showchannels, maxresults, limiting = True ):
		if self.conn is None:
			return 0
		try:
			if limiting:
				sql_cond_limit = self.sql_cond_nofuture + self.sql_cond_minlength
			else:
				sql_cond_limit = ''
			self.logger.info( 'MySQL Query: {}',
				self.sql_query_films +
				' WHERE ' +
				condition +
				sql_cond_limit
			)
			cursor = self.conn.cursor()
			cursor.execute(
				self.sql_query_filmcnt +
				' WHERE ' +
				condition +
				sql_cond_limit +
				( ' LIMIT {}'.format( maxresults + 1 ) if maxresults else '' ),
				params
			)
			( results, ) = cursor.fetchone()
			if maxresults and results > maxresults:
				self.notifier.ShowLimitResults( maxresults )
			cursor.execute(
				self.sql_query_films +
				' WHERE ' +
				condition +
				sql_cond_limit +
				( ' LIMIT {}'.format( maxresults + 1 ) if maxresults else '' ),
				params
			)
			filmui.Begin( showshows, showchannels )
			for ( filmui.id, filmui.title, filmui.show, filmui.channel, filmui.description, filmui.seconds, filmui.size, filmui.aired, filmui.url_sub, filmui.url_video, filmui.url_video_sd, filmui.url_video_hd ) in cursor:
				filmui.Add( totalItems = results )
			filmui.End()
			cursor.close()
			return results
		except mysql.connector.Error as err:
			self.logger.error( 'Database error: {}, {}', err.errno, err )
			self.notifier.ShowDatabaseError( err )
			return 0

	def RetrieveFilmInfo( self, filmid ):
		if self.conn is None:
			return None
		try:
			condition = '( film.id={} )'.format( filmid )
			self.logger.info( 'MySQL Query: {}',
				self.sql_query_films +
				' WHERE ' +
				condition
			)
			cursor = self.conn.cursor()
			cursor.execute(
				self.sql_query_films +
				' WHERE ' +
				condition
			)
			film = Film()
			for ( film.id, film.title, film.show, film.channel, film.description, film.seconds, film.size, film.aired, film.url_sub, film.url_video, film.url_video_sd, film.url_video_hd ) in cursor:
				cursor.close()
				return film
			cursor.close()
		except mysql.connector.Error as err:
			self.logger.error( 'Database error: {}, {}', err.errno, err )
			self.notifier.ShowDatabaseError( err )
		return None

	def GetStatus( self, reconnect = True ):
		status = {
			'modified': int( time.time() ),
			'status': '',
			'lastupdate': 0,
			'filmupdate': 0,
			'fullupdate': 0,
			'add_chn': 0,
			'add_shw': 0,
			'add_mov': 0,
			'del_chn': 0,
			'del_shw': 0,
			'del_mov': 0,
			'tot_chn': 0,
			'tot_shw': 0,
			'tot_mov': 0
		}
		if self.conn is None:
			self.logger.error( 'mysqldb no connection' )

			status['status'] = "UNINIT"
			return status
		try:
			cursor = self.conn.cursor()
			cursor.execute( 'SELECT * FROM `status` LIMIT 1' )
			r = cursor.fetchall()
			cursor.close()
			self.conn.commit()
			if len( r ) == 0:
				status['status'] = "NONE"
				return status
			status['modified']		= r[0][1]
			status['status']	= r[0][2]
			status['lastupdate']	= r[0][3]
			status['filmupdate']	= r[0][4]
			status['fullupdate']		= r[0][5]
			status['add_chn']		= r[0][6]
			status['add_shw']		= r[0][7]
			status['add_mov']		= r[0][8]
			status['del_chn']		= r[0][9]
			status['del_shw']		= r[0][10]
			status['del_mov']		= r[0][11]
			status['tot_chn']		= r[0][12]
			status['tot_shw']		= r[0][13]
			status['tot_mov']		= r[0][14]
			return status
		except mysql.connector.Error as err:
			if err.errno == -1  and reconnect:
				# connection lost. Retry:
				self.logger.warn( 'Database connection lost. Trying to reconnect...' )
				if self.reinit():
					self.logger.info( 'Reconnection successful' )
					return self.GetStatus( False )
			self.logger.error( 'Database error: {}, {}', err.errno, err )
			self.notifier.ShowDatabaseError( err )
			status['status'] = "UNINIT"
			return status

	def UpdateStatus( self, status = None, lastupdate = None, filmupdate = None, fullupdate = None, add_chn = None, add_shw = None, add_mov = None, del_chn = None, del_shw = None, del_mov = None, tot_chn = None, tot_shw = None, tot_mov = None ):
		if self.conn is None:
			return
		if status is None:
			return
		old = self.GetStatus()
		new = self.GetStatus()
		if status is not None:
			new['status'] = status
		if lastupdate is not None:
			new['lastupdate'] = lastupdate
		if filmupdate is not None:
			new['filmupdate'] = filmupdate
		if fullupdate is not None:
			new['fullupdate'] = fullupdate

		if(old['status'] == 'NONE'):
			try:
				cursor = self.conn.cursor()
				# insert status
				cursor.execute("""
					INSERT INTO `status` (
						`id`, `status`, `lastupdate`, `filmupdate`, `fullupdate`
					) VALUES (
						%s, %s, %s, %s, %s
					)
					""", (
						1, status, lastupdate, filmupdate, fullupdate
					)
				)
				cursor.close()
				self.conn.commit()
			except mysql.connector.Error as err:
				self.logger.error('Database error: {}, {}', err.errno, err)
				self.notifier.ShowDatabaseError(err)
				return
			if(status != 'IDLE'):
				return

		if(status != 'IDLE'):
			try:
				cursor = self.conn.cursor()
				# insert status
				cursor.execute("""
					UPDATE `status` 
						set
							`status` = %s, `lastupdate` = %s, `filmupdate` = %s, `fullupdate` = %s
						where id=1
					""", (
						new['status'],
						new['lastupdate'],
						new['filmupdate'],
						new['fullupdate']
					)
				)
				cursor.close()
				self.conn.commit()
			except mysql.connector.Error as err:
				self.logger.error('Database error: {}, {}', err.errno, err)
				self.notifier.ShowDatabaseError(err)
			return


		if tot_chn is not None:
			new['add_chn'] = max(0, tot_chn - old['tot_chn'])
		if tot_shw is not None:
			new['add_shw'] = max(0, tot_shw - old['tot_shw'])
		if tot_mov is not None:
			new['add_mov'] = max(0, tot_mov - old['tot_mov'])
		if tot_chn is not None:
			new['del_chn'] = max(0, old['tot_chn'] - tot_chn)
		if tot_shw is not None:
			new['del_shw'] = max(0, old['tot_shw'] - tot_shw)
		if tot_mov is not None:
			new['del_mov'] = max(0, old['tot_mov'] - tot_mov)
		if tot_chn is not None:
			new['tot_chn'] = tot_chn
		if tot_shw is not None:
			new['tot_shw'] = tot_shw
		if tot_mov is not None:
			new['tot_mov'] = tot_mov
		# TODO: we should only write, if we have changed something...
		new['modified'] = int( time.time() )
		try:
			cursor = self.conn.cursor()
			# insert status
			cursor.execute( """
				UPDATE `status`
					SET `modified`		= %s,
						`status`		= %s,
						`lastupdate`	= %s,
						`filmupdate`	= %s,
						`fullupdate`	= %s,
						`add_chn`		= %s,
						`add_shw`		= %s,
						`add_mov`		= %s,
						`del_chm`		= %s,
						`del_shw`		= %s,
						`del_mov`		= %s,
						`tot_chn`		= %s,
						`tot_shw`		= %s,
						`tot_mov`		= %s
					where id = 1
				""", (
					new['modified'],
					new['status'],
					new['lastupdate'],
					new['filmupdate'],
					new['fullupdate'],
					new['add_chn'],
					new['add_shw'],
					new['add_mov'],
					new['del_chn'],
					new['del_shw'],
					new['del_mov'],
					new['tot_chn'],
					new['tot_shw'],
					new['tot_mov'],
				)
			)
			cursor.close()
			self.conn.commit()
		except mysql.connector.Error as err:
			self.logger.error( 'Database error: {}, {}', err.errno, err )
			self.notifier.ShowDatabaseError( err )

	@staticmethod
	def SupportsUpdate():
		return True

	def reinit( self ):
		self.Exit()
		return self.Init( False, False )

	def ftInit( self ):
		# prevent concurrent updating
		cursor = self.conn.cursor()
		cursor.execute(
			"""
			UPDATE	`status`
			SET		`modified`		= %s,
					`status`		= 'UPDATING'
			WHERE	( `status` != 'UPDATING' )
					OR
					( `modified` < %s )
			""", (
				int( time.time() ),
				int( time.time() ) - 86400
			)
		)
		retval = cursor.rowcount > 0
		self.conn.commit()
		cursor.close()
		self.ft_channel = None
		self.ft_channelid = None
		self.ft_show = None
		self.ft_showid = None
		return retval

	def ftUpdateStart( self, full ):
		param = ( 1, ) if full else ( 0, )
		try:
			cursor = self.conn.cursor()
			cursor.execute('truncate film_import')
			status = self.GetStatus(False)
			return ( status['tot_chn'], status['tot_shw'], status['tot_mov'] )
		except mysql.connector.Error as err:
			self.logger.error( 'Database error: {}, {}', err.errno, err )
			self.notifier.ShowDatabaseError( err )
		return ( 0, 0, 0, )

	def ftUpdateEnd( self, delete ):
		try:
			del_chn = 0
			del_shw = 0
			del_mov = 0
			tot_chn = 0
			tot_shw = 0
			tot_mov = 0

			cursor = self.conn.cursor()
			if delete:
				cursor.execute("""
					delete f1 from film f1
					left join film_import f2
					on f1.idhash = f2.idhash
					where f2.id is null
				""")
				del_mov = cursor.rowcount

			cursor.execute("""
				insert into `channel` (dtCreated, channel)
					select distinct now() dtCreated, fi.`channel`from film_import fi
					left join `channel` c on fi.channel=c.channel
					where c.channel is null
			""")

			cursor.execute("""
				insert into `show` (dtCreated, channelid, `show`, `search`)
					select distinct now() dtCreated, c.`id` channelid, fi.`show`, fi.`showsearch`
					from `channel` c, film_import fi
					left join `show` s on fi.show=s.show
					where fi.channel=c.channel
					and s.show is null
			""")

			cursor.execute("""
				delete c1 from `channel` c1
					inner join `channel` c2
					where c1.id > c2.id
					and c1.channel = c2.channel
			""")
			del_chn = cursor.rowcount

			cursor.execute("""
				delete s1 from `show` s1
					inner join `show` s2
					where s1.id > s2.id
					and s1.search = s2.search
			""")
			del_shw = cursor.rowcount

			cursor.execute("""
				insert into `film` (idhash, dtCreated, channelid, showid, title, `search`,
					aired, duration, website, url_sub, url_video, url_video_sd,
					url_video_hd, airedepoch)
					select distinct fi.idhash, now() dtCreated, c.`id` channelid, s.`id` showid, fi.title, fi.`search`, fi.aired, fi.duration, fi.website, fi.url_sub, fi.url_video, fi.url_video_sd, fi.url_video_hd, fi.airedepoch
						from `channel` c, `show` s , film_import fi
						left join film f on fi.idhash=f.idhash
						where fi.channel=c.channel
						and fi.showsearch=s.search
						and f.idhash is null
			""")

			cursor.execute("""truncate film_import""")

			cursor.close()
			self.conn.commit()

			cursor = self.conn.cursor()
			cursor.execute('SELECT count(*) c FROM `channel`')
			r = cursor.fetchall()
			cursor.close()
			if len(r) == 1:
				tot_chn = r[0][0]

			cursor = self.conn.cursor()
			cursor.execute('SELECT count(*) c FROM `show`')
			r = cursor.fetchall()
			cursor.close()
			if len(r) == 1:
				tot_shw = r[0][0]

			cursor = self.conn.cursor()
			cursor.execute('SELECT count(*) c FROM `film`')
			r = cursor.fetchall()
			cursor.close()
			if len(r) == 1:
				tot_mov = r[0][0]

		except mysql.connector.Error as err:
			self.logger.error( 'Database error: {}, {}', err.errno, err )
			self.notifier.ShowDatabaseError( err )
		return ( del_chn, del_shw, del_mov, tot_chn, tot_shw, tot_mov, )

	def ftInsertFilm( self, film, commit ):
		channel = film['channel'][:64]
		show	= film['show'][:128]
		title	= film['title'][:128]

		hashkey = hashlib.md5("{}:{}:{}".format(channel, show, film['url_video'])).hexdigest()

		try:
			self.sqlValues += """ (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),"""
			self.sqlData += [
				hashkey,
				channel,
				show,
				mvutils.make_search_string(show),
				title,
				mvutils.make_search_string( title ),
				film["aired"],
				film["duration"],
				film["size"],
				film["description"],
				film["website"],
				film["url_sub"],
				film["url_video"],
				film["url_video_sd"],
				film["url_video_hd"],
				film["airedepoch"],
			]

			return (0, 0, 0, 1,)

		except mysql.connector.Error as err:
			self.logger.error( 'Database error: {}, {}', err.errno, err )
			self.notifier.ShowDatabaseError( err )
		return ( 0, 0, 0, 0, )

	def ftFlushInsert(self):
		cursor = self.conn.cursor()
		if len(self.sqlData) > 0:
			sql = self.sqlInsert + self.sqlValues[:-1]
			cursor.execute(sql, self.sqlData)
		cursor.close()
		self.conn.commit()
		self.resetInsertSql()


	def _get_schema_version( self ):
		if self.conn is None:
			return 0
		cursor = self.conn.cursor()
		try:
			cursor.execute( 'SELECT `version` FROM `status` LIMIT 1' )
			( version, ) = cursor.fetchone()
			del cursor
			return version
		except mysql.connector.errors.ProgrammingError:
			return 1
		except mysql.connector.Error as err:
			self.logger.error( 'Database error: {}, {}', err.errno, err )
			self.notifier.ShowDatabaseError( err )
			return 0

	def _handle_database_update( self, convert, version = None ):
		if version is None:
			return self._handle_database_update( convert, self._get_schema_version() )
		if version == 0:
			# should never happen - something went wrong...
			self.Exit()
			return False
		elif version == 3:
			# current version
			return True
		elif convert is False:
			# do not convert (Addon threads)
			#self.Exit()
			self.notifier.ShowUpdatingScheme()
			return False
		elif version == 1:
			# convert from 1 to 2
			self.logger.info( 'Converting database to version 2' )
			self.notifier.ShowUpdateSchemeProgress()
			try:
				cursor = self.conn.cursor()
				cursor.execute( 'SELECT @@SESSION.sql_mode' )
				( sql_mode, ) = cursor.fetchone()
				self.logger.info( 'Current SQL mode is {}', sql_mode )
				cursor.execute( 'SET SESSION sql_mode = ""' )

				self.logger.info( 'Reducing channel name length...' )
				cursor.execute( 'ALTER TABLE `channel` CHANGE COLUMN `channel` `channel` varchar(64) NOT NULL')
				self.notifier.UpdateUpdateSchemeProgress( 5 )
				self.logger.info( 'Reducing show name length...' )
				cursor.execute( 'ALTER TABLE `show` CHANGE COLUMN `show` `show` varchar(128) NOT NULL, CHANGE COLUMN `search` `search` varchar(128) NOT NULL')
				self.notifier.UpdateUpdateSchemeProgress( 10 )
				self.logger.info( 'Reducing film title length...' )
				cursor.execute( 'ALTER TABLE `film` CHANGE COLUMN `title` `title` varchar(128) NOT NULL, CHANGE COLUMN `search` `search` varchar(128) NOT NULL')
				self.notifier.UpdateUpdateSchemeProgress( 65 )
				self.logger.info( 'Deleting old dupecheck index...' )
				cursor.execute( 'ALTER TABLE `film` DROP KEY `dupecheck`')
				self.logger.info( 'Creating and filling new column idhash...' )
				cursor.execute( 'ALTER TABLE `film` ADD COLUMN `idhash` varchar(32) NULL AFTER `id`')
				self.notifier.UpdateUpdateSchemeProgress( 82 )
				cursor.execute( 'UPDATE `film` SET `idhash`= MD5( CONCAT( `channelid`, ":", `showid`, ":", `url_video` ) )')
				self.notifier.UpdateUpdateSchemeProgress( 99 )
				self.logger.info( 'Creating new dupecheck index...' )
				cursor.execute( 'ALTER TABLE `film` ADD KEY `dupecheck` (`idhash`)' )
				self.logger.info( 'Adding version info to status table...' )
				cursor.execute( 'ALTER TABLE `status` ADD COLUMN `version` INT(11) NOT NULL DEFAULT 2')
				self.logger.info( 'Resetting SQL mode to {}', sql_mode )
				cursor.execute( 'SET SESSION sql_mode = %s', ( sql_mode, ) )
				self.logger.info( 'Scheme successfully updated to version 2' )
				return self._handle_database_update(convert, self._get_schema_version())
			except mysql.connector.Error as err:
				self.logger.error( '=== DATABASE SCHEME UPDATE ERROR: {} ===', err )
				self.Exit()
				self.notifier.CloseUpdateSchemeProgress()
				self.notifier.ShowDatabaseError( err )
				return False
		elif version == 2:
			# convert from 2 to 3
			self.logger.info('Converting database to version 3')
			self.notifier.ShowUpdateSchemeProgress()
			try:
				cursor = self.conn.cursor()
				cursor.execute('SELECT @@SESSION.sql_mode')
				(sql_mode,) = cursor.fetchone()
				self.logger.info('Current SQL mode is {}', sql_mode)
				cursor.execute('SET SESSION sql_mode = ""')

				self.logger.info('Dropping touched column on channel...')
				cursor.execute('ALTER TABLE `channel` DROP  `touched`')
				self.notifier.UpdateUpdateSchemeProgress(5)
				self.logger.info('Dropping touched column on show...')
				cursor.execute('ALTER TABLE `show` DROP  `touched`')
				self.notifier.UpdateUpdateSchemeProgress(15)
				self.logger.info('Adding primary key to staus...')
				cursor.execute("ALTER TABLE `status` ADD `id` INT(4) UNSIGNED NOT NULL DEFAULT '1' FIRST, ADD PRIMARY KEY (`id`)")
				self.notifier.UpdateUpdateSchemeProgress(20)
				self.logger.info('Dropping touched column on film...')
				cursor.execute('ALTER TABLE `film` DROP  `touched`')
				self.notifier.UpdateUpdateSchemeProgress(60)

				self.logger.info('Dropping stored procedure ftInsertChannel...')
				cursor.execute('DROP PROCEDURE IF EXISTS `ftInsertChannel`')
				self.notifier.UpdateUpdateSchemeProgress(65)

				self.logger.info('Dropping stored procedure ftInsertFilm...')
				cursor.execute('DROP PROCEDURE IF EXISTS `ftInsertFilm`')
				self.notifier.UpdateUpdateSchemeProgress(70)

				self.logger.info('Dropping stored procedure ftInsertShow...')
				cursor.execute('DROP PROCEDURE IF EXISTS `ftInsertShow`')
				self.notifier.UpdateUpdateSchemeProgress(75)

				self.logger.info('Dropping stored procedure ftUpdateEnd...')
				cursor.execute('DROP PROCEDURE IF EXISTS `ftUpdateEnd`')
				self.notifier.UpdateUpdateSchemeProgress(80)

				self.logger.info('Dropping stored procedure ftUpdateStart...')
				cursor.execute('DROP PROCEDURE IF EXISTS `ftUpdateStart`')
				self.notifier.UpdateUpdateSchemeProgress(85)

				self.logger.info('Creating tabele film_import...')
				cursor.execute("""CREATE TABLE IF NOT EXISTS `film_import` (
					 `id` int(11) NOT NULL AUTO_INCREMENT,
					 `idhash` varchar(32) DEFAULT NULL,
					 `dtCreated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
					 `touched` smallint(1) NOT NULL DEFAULT '1',
					 `channel` varchar(64) NOT NULL,
					 `channelid` int(11) NOT NULL,
					 `show` varchar(128) NOT NULL,
					 `showsearch` varchar(128) NOT NULL,
					 `showid` int(11) NOT NULL,
					 `title` varchar(128) NOT NULL,
					 `search` varchar(128) NOT NULL,
					 `aired` timestamp NULL DEFAULT NULL,
					 `duration` time DEFAULT NULL,
					 `size` int(11) DEFAULT NULL,
					 `description` longtext,
					 `website` varchar(384) DEFAULT NULL,
					 `url_sub` varchar(384) DEFAULT NULL,
					 `url_video` varchar(384) DEFAULT NULL,
					 `url_video_sd` varchar(384) DEFAULT NULL,
					 `url_video_hd` varchar(384) DEFAULT NULL,
					 `airedepoch` int(11) DEFAULT NULL,
					 PRIMARY KEY (`id`),
					 KEY `index_1` (`channel`,`show`),
					 KEY `dupecheck` (`idhash`)
					) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC
				""")
				self.notifier.UpdateUpdateSchemeProgress(95)
				cursor.execute('UPDATE `status` set `version` = 3')
				self.logger.info('Resetting SQL mode to {}', sql_mode)
				cursor.execute('SET SESSION sql_mode = %s', (sql_mode,))
				self.logger.info('Scheme successfully updated to version 3')
				self.notifier.CloseUpdateSchemeProgress()
			except mysql.connector.Error as err:
				self.logger.error('=== DATABASE SCHEME UPDATE ERROR: {} ===', err)
				self.Exit()
				self.notifier.CloseUpdateSchemeProgress()
				self.notifier.ShowDatabaseError(err)
				return False
		return True

	def _handle_database_initialization( self ):
		self.logger.info('Database creation started')

		cursor = None
		dbcreated = False
		try:
			cursor = self.conn.cursor()
			cursor.execute( 'CREATE DATABASE IF NOT EXISTS `{}` DEFAULT CHARACTER SET utf8'.format( self.settings.database ) )
			dbcreated = True
			self.conn.database = self.settings.database
			cursor.execute( 'SET FOREIGN_KEY_CHECKS=0' )
			self.conn.commit()
			cursor.execute("""
				CREATE TABLE `channel` (
					`id`			int(11)			NOT NULL AUTO_INCREMENT,
					`dtCreated`		timestamp		NOT NULL DEFAULT CURRENT_TIMESTAMP,
					`channel`		varchar(64)		NOT NULL,
					PRIMARY KEY						(`id`),
					KEY				`channel`		(`channel`)
				) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8;
			""")
			self.conn.commit()

			cursor.execute( """
				CREATE TABLE `film` (
					`id`			int(11)			NOT NULL AUTO_INCREMENT,
					`idhash`		varchar(32)		DEFAULT NULL,
					`dtCreated`		timestamp		NOT NULL DEFAULT CURRENT_TIMESTAMP,
					`channelid`		int(11)			NOT NULL,
					`showid`		int(11)			NOT NULL,
					`title`			varchar(128)	NOT NULL,
					`search`		varchar(128)	NOT NULL,
					`aired`			timestamp		NULL DEFAULT NULL,
					`duration`		time			DEFAULT NULL,
					`size`			int(11)			DEFAULT NULL,
					`description`	longtext,
					`website`		varchar(384)	DEFAULT NULL,
					`url_sub`		varchar(384)	DEFAULT NULL,
					`url_video`		varchar(384)	DEFAULT NULL,
					`url_video_sd`	varchar(384)	DEFAULT NULL,
					`url_video_hd`	varchar(384)	DEFAULT NULL,
					`airedepoch`	int(11)			DEFAULT NULL,
					PRIMARY KEY						(`id`),
					KEY				`index_1`		(`showid`,`title`),
					KEY				`index_2`		(`channelid`,`title`),
					KEY				`dupecheck`		(`idhash`),
					CONSTRAINT `FK_FilmChannel` FOREIGN KEY (`channelid`) REFERENCES `channel` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
					CONSTRAINT `FK_FilmShow` FOREIGN KEY (`showid`) REFERENCES `show` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
				) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8;
			""" )
			self.conn.commit()
			cursor.execute("""
				CREATE TABLE IF NOT EXISTS `film_import` (
					`id` int(11) NOT NULL AUTO_INCREMENT,
					`idhash` varchar(32) DEFAULT NULL,
					`dtCreated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
					`touched` smallint(1) NOT NULL DEFAULT '1',
					`channel` varchar(64) NOT NULL,
					`channelid` int(11) NOT NULL,
					`show` varchar(128) NOT NULL,
					`showsearch` varchar(128) NOT NULL,
					`showid` int(11) NOT NULL,
					`title` varchar(128) NOT NULL,
					`search` varchar(128) NOT NULL,
					`aired` timestamp NULL DEFAULT NULL,
					`duration` time DEFAULT NULL,
					`size` int(11) DEFAULT NULL,
					`description` longtext,
					`website` varchar(384) DEFAULT NULL,
					`url_sub` varchar(384) DEFAULT NULL,
					`url_video` varchar(384) DEFAULT NULL,
					`url_video_sd` varchar(384) DEFAULT NULL,
					`url_video_hd` varchar(384) DEFAULT NULL,
					`airedepoch` int(11) DEFAULT NULL,
					PRIMARY KEY (`id`),
					KEY `index_1` (`channel`,`show`),
					KEY `dupecheck` (`idhash`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC
			""")
			self.conn.commit()
			cursor.execute( """
				CREATE TABLE `show` (
					`id`			int(11)			NOT NULL AUTO_INCREMENT,
					`dtCreated`		timestamp		NOT NULL DEFAULT CURRENT_TIMESTAMP,
					`channelid`		int(11)			NOT NULL,
					`show`			varchar(128)	NOT NULL,
					`search`		varchar(128)	NOT NULL,
					PRIMARY KEY						(`id`),
					KEY				`show`			(`show`),
					KEY				`search`		(`search`),
					KEY				`combined_1`	(`channelid`,`search`),
					KEY				`combined_2`	(`channelid`,`show`),
					CONSTRAINT `FK_ShowChannel` FOREIGN KEY (`channelid`) REFERENCES `channel` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
				) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8;
			""" )
			self.conn.commit()

			cursor.execute( """
				CREATE TABLE `status` (
				 `id` int(4) unsigned NOT NULL DEFAULT '1',
				 `modified` int(11) NOT NULL,
				 `status` varchar(255) NOT NULL,
				 `lastupdate` int(11) NOT NULL,
				 `filmupdate` int(11) NOT NULL,
				 `fullupdate` int(1) NOT NULL,
				 `add_chn` int(11) NOT NULL,
				 `add_shw` int(11) NOT NULL,
				 `add_mov` int(11) NOT NULL,
				 `del_chm` int(11) NOT NULL,
				 `del_shw` int(11) NOT NULL,
				 `del_mov` int(11) NOT NULL,
				 `tot_chn` int(11) NOT NULL,
				 `tot_shw` int(11) NOT NULL,
				 `tot_mov` int(11) NOT NULL,
				 `version` int(11) NOT NULL DEFAULT '3',
				 PRIMARY KEY (`id`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC
			""" )
			self.conn.commit()

			cursor.execute( 'INSERT INTO `status` VALUES (1, 0,"IDLE",0,0,0,0,0,0,0,0,0,0,0,0,3)' )
			self.conn.commit()

			cursor.execute( 'SET FOREIGN_KEY_CHECKS=1' )
			self.conn.commit()

			cursor.close()
			self.logger.info( 'Database creation successfully completed' )
			return True
		except mysql.connector.Error as err:
			self.logger.error( '=== DATABASE CREATION ERROR: {} ===', err )
			self.notifier.ShowDatabaseError( err )
			try:
				if dbcreated:
					cursor.execute( 'DROP DATABASE `{}`'.format( self.settings.database ) )
					self.conn.commit()
				if cursor is not None:
					cursor.close()
					del cursor
				if self.conn is not None:
					self.conn.close()
					self.conn = None
			except mysql.connector.Error as err:
				# should never happen
				self.conn = None
		return False
