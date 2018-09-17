package com.writer;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * This writer will write items in batch and upon error, try to write record
 * individually when retryOnErrorInBatchWriting is true
 * 
 * @author sahasuba
 * 
 */

@Log4j2
public class JdbcBatchWriter<T> implements InitializingBean {

	protected NamedParameterJdbcOperations namedParameterJdbcTemplate;

	protected PreparedStatementSetter preparedStatementSetter;

	protected String sql;

	protected boolean assertUpdates = true;

	protected boolean usingNamedParameters = true;

	@Getter
	@Setter
	protected boolean retryOnErrorInBatchWriting = false;

	/**
	 * Public setter for the flag that determines whether an assertion is made
	 * that all items cause at least one row to be updated.
	 * 
	 * @param assertUpdates
	 *            the flag to set. Defaults to true;
	 */
	public void setAssertUpdates(boolean assertUpdates) {
		this.assertUpdates = assertUpdates;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public void setPreparedStatementSetter(PreparedStatementSetter preparedStatementSetter) {
		this.preparedStatementSetter = preparedStatementSetter;
	}

	/**
	 * Public setter for the data source for injection purposes.
	 *
	 * @param dataSource
	 *            {@link javax.sql.DataSource} to use for querying against
	 */
	public void setDataSource(DataSource dataSource) {
		if (namedParameterJdbcTemplate == null) {
			this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
		}
	}

	/**
	 * Public setter for the {@link NamedParameterJdbcOperations}.
	 * 
	 * @param namedParameterJdbcTemplate
	 *            the {@link NamedParameterJdbcOperations} to set
	 */
	public void setJdbcTemplate(NamedParameterJdbcOperations namedParameterJdbcTemplate) {
		this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
	}

	/**
	 * Check mandatory properties - there must be a SimpleJdbcTemplate and an
	 * SQL statement plus a parameter source.
	 */
	@Override
	public void afterPropertiesSet() {
		Assert.notNull(namedParameterJdbcTemplate, "A DataSource or a NamedParameterJdbcTemplate is required.");
		Assert.notNull(sql, "An SQL statement is required.");
		if (!usingNamedParameters) {
			Assert.notNull(preparedStatementSetter,
					"Using SQL statement with '?' placeholders requires an preparedStatementSetter");
		}
	}

	/**
	 * write records in batch primarily
	 * 
	 * @param items
	 * @param initialOffset
	 * @param finalOffset
	 * @param topic
	 * @param partition
	 * @throws Exception
	 */
	public void write(final List<? extends T> items, final long initialOffset, final long finalOffset,
			final String topic, final int partition) throws Exception {

		if (!items.isEmpty()) {

			int[] updateCounts = null;

			try {
				if (usingNamedParameters) {
					if (items.get(0) instanceof Map) {
						updateCounts = namedParameterJdbcTemplate.batchUpdate(sql, items.toArray(new Map[0]));
					}
				}
			}
			// in case of exception in batch writing which may cause due to
			// single or few bad records, try to write each record to ensure
			// maximum possible successful write
			catch (Exception e) {
				if (retryOnErrorInBatchWriting) {
					log.error("Exception {} in writing batch of {} items, so trying to write records individually", e,
							items.size());
					for (int i = 0; i < items.size(); i++) {
						writeIndividualRecord(Collections.singletonList(items.get(i)), initialOffset, finalOffset,
								topic, partition);
					}

				} else {
					log.error("Exception {} in writing records beween offset {} and {} for topic {} and partition {} ",
							e, initialOffset, finalOffset, topic, partition);
				}
				return;
			}
			if (assertUpdates) {
				for (int i = 0; i < updateCounts.length; i++) {
					int value = updateCounts[i];
					if (value == 0) {
						throw new EmptyResultDataAccessException("Item " + i + " of " + updateCounts.length
								+ " did not update any rows: [" + items.get(i) + "]", 1);
					}
				}
			}
		}
	}

	/**
	 * write individual record in case of failure in batch writing
	 * 
	 * @param items
	 * @param initialOffset
	 * @param finalOffset
	 * @param topic
	 * @param partition
	 * @throws Exception
	 */
	protected void writeIndividualRecord(final List<? extends T> items, final long initialOffset,
			final long finalOffset, final String topic, final int partition) throws Exception {
		try {
			if (!items.isEmpty()) {

				int[] updateCounts = null;

				if (usingNamedParameters) {
					if (items.get(0) instanceof Map) {
						updateCounts = namedParameterJdbcTemplate.batchUpdate(sql, items.toArray(new Map[0]));
					}
				}
				if (assertUpdates) {
					for (int i = 0; i < updateCounts.length; i++) {
						int value = updateCounts[i];
						if (value == 0) {
							throw new EmptyResultDataAccessException("Item " + i + " of " + updateCounts.length
									+ " did not update any rows: [" + items.get(i) + "]", 1);
						}
					}
				}
			}
		} catch (Exception e) {
			log.error("Exception {} in writing record {} beween offset {} and {} for topic {} and partition {} ", e,
					items.get(0), initialOffset, finalOffset, topic, partition);
		}
	}
}