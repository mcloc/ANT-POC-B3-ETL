package br.com.assemblenewtechnologies.ANTLogSync.rotines;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.Helpers.DBConnectionHelper;
import br.com.assemblenewtechnologies.ANTLogSync.model.CsvLoadLot;

public class Etl extends AbstractRotine {
	private static Logger LOGGER = LoggerFactory.getLogger(Etl.class);

	public void etl0_sanity_check() throws Exception {
		// verificar se DATA RELOGIO BATE COM DATA LOTE isso resolve sanity de
		// mes no dia e dia no mes
		// (rejeita ida pra hot_table)

		// verificar ordem de grandeza dos numeros
		// (rejeita ida pra hot_table)

		// verificar dropout (mais conhecido por dropstep)
		// (aceita ida pra hot_table e LOGA o DROPOUT. Tem uma tabela so de droupout
		// cas rastreabilidade do load LOT e arquivo)

		LOGGER.info("[ETL] etl0_sanity_check...");
	}

	private void etl1_populate_assets() throws Exception {
		LOGGER.info("[ETL]etl1_populate_assets ...");

		if (1 == 1)
			return; // DEBUG

		long start_time = System.currentTimeMillis();
		LOGGER.info("Initializing B3 SignalLogger ETL phase 1 - 'etl1_populate_assets' ...");

		// extract group by all assets to assets table
		// PL SQL
		Connection connection;

		try {
			connection = DBConnectionHelper.getNewConn();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new Exception("No database connection...");
		}

		String status_finished = CsvLoadLot.getFinishedStatusStringCommaSeparated();
		Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet rs = stmt.executeQuery(
				"select * from Intellect.csv_load_lot where status in (" + status_finished + ") order by lot_name ASC");

		List<Integer> csv_lot_finished = new ArrayList<Integer>();
		while (rs.next()) {
			csv_lot_finished.add(rs.getInt("id"));
		}

		stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		rs = stmt.executeQuery("select * from B3Log.B3AtivosOpcoes");

		Map<String, String> ativo_opcoes_db = new HashMap<String, String>();
		while (rs.next()) {
			ativo_opcoes_db.put(rs.getString("asset"), rs.getString("opcao_ativo"));
		}

		//FOR EACH FINISHED LOT
		for (Integer csv_lot_id : csv_lot_finished) {
			try {
				LOGGER.info("Fetching assets from B3Log.B3SignalLoggerRaw:");
				stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				rs = stmt.executeQuery("select asset, substring(asset, '[A-Z]+') as substr_ativo \n"
						+ "from B3Log.B3SignalLoggerRaw  \n" + "WHERE strike = 0\n" + "group by 1,2\n"
						+ "and lot_id = " + csv_lot_id + " \n"
						+ "order by 1,2");
				int rows = 0;
				if (rs.last()) {
					rows = rs.getRow();
					rs.beforeFirst();
					LOGGER.info("total assets found " + rows);

					if (rows == 0)
						return;
				}

				PreparedStatement preparedStatement;
				while (rs.next()) {
					// Asset and Option already on DB
					if (ativo_opcoes_db.containsKey(rs.getString("asset")))
						continue;

					Statement stmt2 = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
							ResultSet.CONCUR_READ_ONLY);
					String sql = "select '" + rs.getString("asset") + "' as ativo, '" + rs.getString("substr_ativo")
							+ "' as substr_ativo, asset as opcao_ativo \n" + "from B3Log.B3SignalLoggerRaw  \n"
							+ "WHERE lot_id = "+csv_lot_id+" AND strike != 0 AND asset like '" + rs.getString("substr_ativo") + "%' \n"
							+ "group by 1,2,3\n" + "order by 1,3";
					LOGGER.info(sql);
					ResultSet rs2 = stmt2.executeQuery(sql);

					while (rs2.next()) {
						String compiledQuery = "INSERT INTO B3Log.B3AtivosOpcoes("
								+ "ativo,substr_opcao_ativo,opcao_ativo) VALUES" + "(?, ?, ?)";
						preparedStatement = connection.prepareStatement(compiledQuery);
						preparedStatement.setString(1, rs2.getString("ativo"));
						preparedStatement.setString(2, rs2.getString("substr_ativo"));
						preparedStatement.setString(3, rs2.getString("opcao_ativo"));
						preparedStatement.execute();
						preparedStatement.clearParameters();
						preparedStatement.close();
					}

				}

				connection.close();
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
				connection.close();
				// e.printStackTrace();
				throw e;
			}
		}

		long _now = System.currentTimeMillis();
		long _diff_time = _now - start_time;
		LOGGER.info("Total time to populate assets: " + _diff_time + "ms ");

	}

	public void etl1_normalization() throws Exception {
		LOGGER.info("[ETL] etl1_normalization...");
		LOGGER.info("B3Log.B3SignalLoggerRaw GROUP BY CHANGES");
		LOGGER.info("Results will be at B3Log.B3LogSignalUnique table.");
		// if diferente manda pra hot_table order by relogio
	}

	public void etl2_preco_medio() throws Exception {
		// volume /[dividido] qtd negocios de cada tupla da hot_table
		LOGGER.info("[ETL] etl2_preco_medio...");
	}

	public void etl2_black_scholes() throws Exception {
		// de cada tupla da hot_table
		// quero coluna tmb com Logaritimo da variancia esse log eh um vetor de angulo e
		// força
		LOGGER.info("[ETL]etl2_black_scholes ...");
	}

	@Override
	public void handler_start() throws Exception {
		try {
			etl1_populate_assets();
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			throw e;
		}

	}

	@Override
	public void handler_finish() throws Exception {
		// TODO Auto-generated method stub

	}
}
