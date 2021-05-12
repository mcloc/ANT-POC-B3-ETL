package br.com.assemblenewtechnologies.ANTLogSync.rotines;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Etl extends AbstractRotine {
	private static Logger LOGGER = LoggerFactory.getLogger(Etl.class);
	
	public void etl0_sanity_check() throws Exception{
		//verificar se DATA RELOGIO BATE COM DATA LOTE isso resolve sanity de 
		//mes no dia e dia no mes 
		//(rejeita ida pra  hot_table)
		
		//verificar ordem de grandeza dos numeros
		//(rejeita ida pra  hot_table)
		
		//verificar dropout (mais conhecido por dropstep)
		//(aceita ida pra  hot_table e LOGA o DROPOUT. Tem uma tabela so de droupout
		//cas rastreabilidade do load LOT e arquivo)
		
		LOGGER.info("[ETL] etl0_sanity_check...");
	}
	public void etl1_populate_assets() throws Exception{
		LOGGER.info("[ETL]etl1_populate_assets ...");
		//extract group by all assets to assets table
		//PL SQL
		
		
		
	}
	public void etl1_normalization() throws Exception{
		LOGGER.info("[ETL] etl1_normalization...");
		//if diferente manda pra hot_table order by relogio
	}
	public void etl2_preco_medio() throws Exception{
		// volume /[dividido] qtd negocios de cada tupla da hot_table
		LOGGER.info("[ETL] etl2_preco_medio...");
	}
	public void etl2_black_scholes() throws Exception{
		//de cada tupla da hot_table
		//quero coluna tmb com Logaritimo da variancia esse log eh um vetor de angulo e força
		LOGGER.info("[ETL]etl2_black_scholes ...");
	}
	@Override
	public void handler_start() throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void handler_finish() throws Exception {
		// TODO Auto-generated method stub
		
	}
}
