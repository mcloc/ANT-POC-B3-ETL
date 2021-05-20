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
import br.com.assemblenewtechnologies.ANTLogSync.process_handlers.CSVHandler;
import br.com.assemblenewtechnologies.ANTLogSync.process_handlers.ETLHandler;

public class Etl extends AbstractRotine {
	private static Logger LOGGER = LoggerFactory.getLogger(Etl.class);
	private long start_time;
	private String thread_name = "ETL_HANDLER";
	private Thread thread;
	private ETLHandler runnable;

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

	public void etl1_populate_assets() throws Exception {
		long start_time = System.currentTimeMillis();
		LOGGER.debug("Initializing B3 SignalLogger ETL phase 1 - 'etl1_populate_assets' ...");

		Connection connection;
		Statement stmt;
		ResultSet rs;
		try {
			connection = DBConnectionHelper.getNewConn();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new Exception("No database connection...");
		}

		List<BigDecimal> csv_lot_finished = getFinishedLots(connection);
		Map<String, String> ativo_opcoes_db = getAssetsInDB(connection);

		// FOR EACH FINISHED LOT
		for (BigDecimal csv_lot_id : csv_lot_finished) {
			CsvLoadLot csv_lot = CsvLoadLot.getLotByLotId(csv_lot_id);
			try {
				LOGGER.debug("Fetching assets from B3Log.B3SignalLoggerRaw:");
				stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				rs = stmt.executeQuery("select asset, substring(asset, '[A-Z]+') as substr_ativo \n"
						+ "from B3Log.B3SignalLoggerRaw  \n" + "WHERE strike = 0\n" + "and lot_id = " + csv_lot_id
						+ "  \n" + "group by 1,2\n" + "order by 1,2");
				int rows = 0;
				if (rs.last()) {
					rows = rs.getRow();
					rs.beforeFirst();
					LOGGER.debug("total assets found " + rows + " for this lot: " + csv_lot.getLot_name());

					if (rows == 0)
						return;
				}
				List<String> _inserted_ativo = new ArrayList<String>();
				Map<String, String> _ativos = new HashMap<String, String>();
				while (rs.next()) {

					if (rs.getString("asset").equals("B3SA3"))
						continue;

					// Assets and Option already on DB
					if (ativo_opcoes_db.containsKey(rs.getString("asset")))
						continue;
					
					_ativos.put(rs.getString("asset"), rs.getString("substr_ativo"));
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
		             insertNewAssetsInDB(connection, _inserted_ativo, _ativos_options, _ativo, _ativo_substr);
		        }
		        
		        //TODO: INSERT ASSETS WITH NO DERIVATIVES (strike = 0 and none != 0) they are missing
//		        itr = _ativos.entrySet().iterator();
//		        while(itr.hasNext())
//		        {
//		        	if(_inserted_ativo.add(_ativo);)
//		        }
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
				// e.printStackTrace();
				throw e;
			}

			//INCREMENT CSV LOT STATUS FINISHED TO +1 
			int csv_status = csv_lot.getStatus();
			if (csv_status < 0)
				csv_lot.changeStatus(csv_lot.getStatus() - 1);
			else
				csv_lot.changeStatus(csv_lot.getStatus() + 1);

		} // END LOOP CSV FINISHED LOTS

		long _now = System.currentTimeMillis();
		long _diff_time = _now - start_time;
		LOGGER.debug("Total time to populate assets: " + _diff_time + "ms ");
	}

	private void insertNewAssetsInDB(Connection connection, List<String> _inserted_ativo, List<String> _ativos_options,
			String _ativo, String _ativo_substr) throws SQLException {
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
				 _inserted_ativo.add(_ativo);
			 }
		 }
	}

	private Map<String, String> getAssetsInDB(Connection connection) throws SQLException {
		Statement stmt;
		ResultSet rs;
		//GET ASSESTS ALREADY IN DB
		stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		rs = stmt.executeQuery("select * from B3Log.B3AtivosOpcoes");
		Map<String, String> ativo_opcoes_db = new HashMap<String, String>();
		while (rs.next()) {
			ativo_opcoes_db.put(rs.getString("ativo"), rs.getString("opcao_ativo"));
		}
		return ativo_opcoes_db;
	}

	private List<BigDecimal> getFinishedLots(Connection connection) throws SQLException {
		//GET ALL LOTs STATUS FINISHED TO INITIATE ETL PHASE 1 
		String status_finished = CsvLoadLot.getFinishedStatusStringCommaSeparated();
		Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet rs = stmt.executeQuery(
				"select * from Intellect.csv_load_lot where status in (" + status_finished + ") order by lot_name ASC");
		List<BigDecimal> csv_lot_finished = new ArrayList<BigDecimal>();
		while (rs.next()) {
			csv_lot_finished.add(rs.getBigDecimal("id"));
		}
		return csv_lot_finished;
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
		runnable = new ETLHandler(this);
		thread = new Thread(runnable, thread_name );
		thread.start();
	}

	@Override
	public void handler_finish() throws Exception {
		thread.interrupt();
	}

	public void setStartTime() {
		this.start_time = System.currentTimeMillis();
		
	}
}
