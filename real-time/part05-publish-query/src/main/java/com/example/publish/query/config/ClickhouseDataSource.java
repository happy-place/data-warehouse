package com.example.publish.query.config;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

@Configuration//注解到spring容器中
@MapperScan(basePackages = ClickhouseDataSource.PACKAGE, sqlSessionFactoryRef = ClickhouseDataSource.SESSION_FACTORY_REF)
public class ClickhouseDataSource {

    static final String PACKAGE = "com.example.publish.query.mapper.clickhouse";
    private static final String MAPPER_LOCATION = "classpath:mapper/clickhouse/*.xml";
    static final String SESSION_FACTORY_REF = "clickhouseSqlSessionFactory";

//    @Value("clickhouse.datasource.jdbcUrl")
//    private String jdbcUrl;
//
//    @Value("clickhouse.datasource.driverClassName")
//    private String driverClassName;

    /**
     * 返回clickhouse数据库的数据源
     * @return
     */
    @Bean(name="clickhouseSource")
     @ConfigurationProperties(prefix = "clickhouse.datasource")
    public DataSource dataSource(){
        return DataSourceBuilder.create()
                .build();
    }

    /**
     * 返回clickhouse数据库的会话工厂
     * @param ds
     * @return
     * @throws Exception
     */
    @Bean(name = "clickhouseSqlSessionFactory")
    public SqlSessionFactory sqlSessionFactory(@Qualifier("clickhouseSource") DataSource ds) throws Exception{
        SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
        bean.setDataSource(ds);
        bean.setMapperLocations(new PathMatchingResourcePatternResolver().getResources(ClickhouseDataSource.MAPPER_LOCATION));
        return bean.getObject();
    }

    /**
     * 返回clickhouse数据库的会话模板
     * @param sessionFactory
     * @return
     * @throws Exception
     */
    @Bean(name = "clickhouseSqlSessionTemplate")
    public SqlSessionTemplate sqlSessionTemplate(@Qualifier("clickhouseSqlSessionFactory") SqlSessionFactory sessionFactory) throws  Exception{
        return  new SqlSessionTemplate(sessionFactory);
    }

    /**
     * 返回clickhouse数据库的事务
     * @param ds
     * @return
     */
    @Bean(name = "clickhouseTransactionManager")
    public DataSourceTransactionManager transactionManager(@Qualifier("clickhouseSource") DataSource ds){
        return new DataSourceTransactionManager(ds);
    }
}

