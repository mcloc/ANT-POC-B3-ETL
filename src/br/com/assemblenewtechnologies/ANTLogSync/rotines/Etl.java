package br.com.assemblenewtechnologies.ANTLogSync.rotines;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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

//		if (1 == 1)
//			return; // DEBUG

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

		List<BigDecimal> csv_lot_finished = new ArrayList<BigDecimal>();
		while (rs.next()) {
			csv_lot_finished.add(rs.getBigDecimal("id"));
		}

		stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		rs = stmt.executeQuery("select * from B3Log.B3AtivosOpcoes");

		Map<String, String> ativo_opcoes_db = new HashMap<String, String>();
		while (rs.next()) {
			ativo_opcoes_db.put(rs.getString("ativo"), rs.getString("opcao_ativo"));
		}

		// FOR EACH FINISHED LOT
		for (BigDecimal csv_lot_id : csv_lot_finished) {
			try {
				LOGGER.info("Fetching assets from B3Log.B3SignalLoggerRaw:");
				stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				rs = stmt.executeQuery("select asset, substring(asset, '[A-Z]+') as substr_ativo \n"
						+ "from B3Log.B3SignalLoggerRaw  \n" + "WHERE strike = 0\n" + "and lot_id = " + csv_lot_id
						+ "  \n" + "group by 1,2\n" + "order by 1,2");
				int rows = 0;
				if (rs.last()) {
					rows = rs.getRow();
					rs.beforeFirst();
					LOGGER.info("total assets found " + rows);

					if (rows == 0)
						return;
				}

				Map<String, String> _ativos = new HashMap<String, String>();
//				PreparedStatement preparedStatement;
				while (rs.next()) {

					if (rs.getString("asset").equals("B3SA3"))
						continue;

					// Asset and Option already on DB
					if (ativo_opcoes_db.containsKey(rs.getString("asset")))
						continue;
					
					
					_ativos.put(rs.getString("asset"), rs.getString("substr_ativo"));
					

//					Statement stmt2 = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
//							ResultSet.CONCUR_READ_ONLY);
//					String sql = "INSERT INTO B3Log.B3AtivosOpcoes(\n" + "ativo,substr_opcao_ativo,opcao_ativo)\n"
//							+ "select '" + rs.getString("asset") + "' as ativo, '" + rs.getString("substr_ativo")
//							+ "' as substr_ativo, asset as opcao_ativo \n" + "from B3Log.B3SignalLoggerRaw  \n"
//							+ "WHERE lot_id = " + csv_lot_id + " AND strike != 0 AND asset like '"
//							+ rs.getString("substr_ativo") + "%' \n" + "group by 1,2,3\n" + "order by 1,3";
////					LOGGER.info(sql);
//					stmt2.execute(sql);
//					ResultSet rs2 = stmt2.executeQuery(sql);
//
//					while (rs2.next()) {
//						String compiledQuery = "INSERT INTO B3Log.B3AtivosOpcoes("
//								+ "ativo,substr_opcao_ativo,opcao_ativo) VALUES" + "(?, ?, ?)";
//						preparedStatement = connection.prepareStatement(compiledQuery);
//						preparedStatement.setString(1, rs2.getString("ativo"));
//						preparedStatement.setString(2, rs2.getString("substr_ativo"));
//						preparedStatement.setString(3, rs2.getString("opcao_ativo"));
//						preparedStatement.execute();
//						preparedStatement.clearParameters();
////						preparedStatement.close();
//					}

				}

				stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				rs = stmt.executeQuery("select asset as opcao \n"
						+ "from B3Log.B3SignalLoggerRaw  \n" + "WHERE strike != 0\n" + "and lot_id = " + csv_lot_id
						+ "  \n" + "group by 1\n" + "order by 1");
				rows = 0;
				if (rs.last()) {
					rows = rs.getRow();
					rs.beforeFirst();
					LOGGER.info("total asset options found " + rows);

					if (rows == 0)
						return;
				}
				
				List<String> _ativos_options = new ArrayList<String>();
//				PreparedStatement preparedStatement;
				while (rs.next()) {

					if (rs.getString("opcao").equals("B3SA3"))
						continue;

					// Asset and Option already on DB
					if (ativo_opcoes_db.containsKey(rs.getString("opcao")))
						continue;
					
					
					_ativos_options.add(rs.getString("opcao"));
				}
				
				Iterator<Map.Entry<String, String>> itr = _ativos.entrySet().iterator();
		         
		        while(itr.hasNext())
		        {
		             Map.Entry<String, String> entry = itr.next();
		             String _ativo = entry.getKey();
		             String _ativo_substr = entry.getValue();
		             PreparedStatement preparedStatement;
		             for(String _option : _ativos_options) {
		            	 if(_option.contains(_ativo_substr)) {
		            		 
		            			String sql = "INSERT INTO B3Log.B3AtivosOpcoes(\n" + "ativo,substr_opcao_ativo,opcao_ativo)\n"
		    							+ "VALUES (?,?,?)";
//		    					LOGGER.info(sql);
		    							
							preparedStatement = connection.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);
							preparedStatement.setString(1, _ativo);
							preparedStatement.setString(2, _ativo_substr);
							preparedStatement.setString(3, _option);
							preparedStatement.execute();
		            	 }
		             }
		        }
				
				
//				connection.close();
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
//				connection.close();
				// e.printStackTrace();
				throw e;
			}

			CsvLoadLot csv_lot = CsvLoadLot.getLotByLotId(csv_lot_id);
			int csv_status = csv_lot.getStatus();
			if (csv_status < 0)
				csv_lot.changeStatus(csv_lot.getStatus() - 1);
			else
				csv_lot.changeStatus(csv_lot.getStatus() + 1);

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
