import pandas as pd

class Acomph():

    # Pega valores que não estão no excel do Acomph
    @staticmethod
    def get_others_acomph(df, connection):
        janela_data = df.data.unique()

        # --------------------------------------------------------------
        # ------------------------ BACIA GRANDE ------------------------
        # --------------------------------------------------------------

        posto_1 = df.loc[df.cod_posto == 1, ['data', 'vazao_natural']].set_index('data')

        # Itutinga ( (1) )
        posto = 2
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = posto_1.reset_index(drop=True)
        df = df.append(posto_df)

        # --------------------------------------------------------------
        # ----------------------- BACIA PARAGUAI -----------------------
        # --------------------------------------------------------------

        posto_259 = df.loc[df.cod_posto == 259, ['data', 'vazao_natural']].set_index('data')

        # Itiquira II ( (259) )
        posto = 252
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = posto_259.reset_index(drop=True)
        df = df.append(posto_df)

        # --------------------------------------------------------------
        # --------------------- BACIA SÃO FRANCISCO --------------------
        # --------------------------------------------------------------
        posto_169 = df.loc[df.cod_posto == 169, ['data', 'vazao_incremental']].set_index('data')
        posto_172 = df.loc[df.cod_posto == 172, ['data', 'vazao_incremental']].set_index('data')

        # Sobradinho Incremental ( (169) )
        posto = 168
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_incremental'] = posto_169.reset_index(drop=True)
        posto_df['ena'] = (posto_df.vazao_incremental * 0).reset_index(drop=True)
        df = df.append(posto_df)

        # Itaparica Incremental ( (172) )
        posto = 171
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_incremental'] = posto_172.reset_index(drop=True)
        posto_df['ena'] = (posto_df.vazao_incremental * 0).reset_index(drop=True)
        df = df.append(posto_df)

        # --------------------------------------------------------------
        # -------------------- BACIA PARAÍBA DO SUL --------------------
        # --------------------------------------------------------------

        # Cálculo do posto regredido Santana Nat.
        def get_posto_regredido_203(p_201):
            m = pd.to_datetime(p_201.name).month
            vazao = p_201

            if m == 1:
                return -0.4 + 1.476 * vazao
            if m == 2:
                return 0.4 + 1.449 * vazao
            if m == 3:
                return -0.2 + 1.477 * vazao
            if m == 4:
                return 0.3 + 1.453 * vazao
            if m == 5:
                return 1.7 + 1.32 * vazao
            if m == 6:
                return 0.3 + 1.419 * vazao
            if m == 7:
                return 0.2 + 1.436 * vazao
            if m == 8:
                return 0 + 1.462 * vazao
            if m == 9:
                return -0.1 + 1.477 * vazao
            if m == 10:
                return -0.1 + 1.467 * vazao
            if m == 11:
                return 0.1 + 1.457 * vazao
            if m == 12:
                return 0.1 + 1.457 * vazao

        posto_125 = df.loc[df.cod_posto == 125, ['data', 'vazao_natural']].set_index('data')
        posto_129 = df.loc[df.cod_posto == 129, ['data', 'vazao_natural']].set_index('data')
        posto_130 = df.loc[df.cod_posto == 130, ['data', 'vazao_natural']].set_index('data')
        posto_201 = df.loc[df.cod_posto == 201, ['data', 'vazao_natural']].set_index('data')
        posto_202 = df.loc[df.cod_posto == 202, ['data', 'vazao_natural']].set_index('data')
        
        # Santana Nat. ( Posto Regredido )
        posto = 203
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_201.apply(lambda row: get_posto_regredido_203(row), axis=1)).reset_index(drop=True)
        df = df.append(posto_df)

        posto_203 = df.loc[df.cod_posto == 203, ['data', 'vazao_natural']].set_index('data')

        # Bomb. S. Cecília ( Se (125) <= 190: [(125) * 119] / 190
        #                    Se (125) <= 209: 119
        #                    Se (125) <= 250: (125) - 90
        #                    Se (125) > 250: 160 )
        posto = 298
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})

        def bomb_s_cecilia(p_125):
            if p_125 <= 190:
                return (p_125 * 119) / 190
            if p_125 <= 209:
                return 119
            if p_125 <= 250:
                return p_125 - 90
            if p_125 > 250:
                return 160
        
        posto_df['vazao_natural'] = (posto_125.vazao_natural.apply(lambda x: bomb_s_cecilia(x))).reset_index(drop=True)
        df = df.append(posto_df)

        posto_298 = df.loc[df.cod_posto == 298, ['data', 'vazao_natural']].set_index('data')

        # Santana ( (203) – (201) + (298) + max [ 0 ; (201) - 25 ] ) 
        posto = 315
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_203.vazao_natural - posto_201.vazao_natural + posto_298.vazao_natural + posto_201.vazao_natural.apply(
            lambda x: max(0, x-25)
        )).reset_index(drop=True)
        df = df.append(posto_df)

        posto_315 = df.loc[df.cod_posto == 315, ['data', 'vazao_natural']].set_index('data')

        # Vigário ( Mín [(315); 190 m3/s] ) 
        posto = 316
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_315.vazao_natural.apply(lambda x: min(x, 190))).reset_index(drop=True)
        df = df.append(posto_df)

        posto_316 = df.loc[df.cod_posto == 316, ['data', 'vazao_natural']].set_index('data')

        # Vertimento Santana ( (315) – (316) )
        posto = 304
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_315 - posto_316).reset_index(drop=True)
        df = df.append(posto_df)

        posto_304 = df.loc[df.cod_posto == 304, ['data', 'vazao_natural']].set_index('data')

        # Ilha dos Pombos ( (130) – (298) - (203) + (304) ) 
        posto = 299
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_130 - posto_298 - posto_203 + posto_304).reset_index(drop=True)
        df = df.append(posto_df)

        # Anta ( (129) – (298) – (203) + (304) )
        posto = 127
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_129 - posto_298 - posto_203 + posto_304).reset_index(drop=True)
        df = df.append(posto_df)

        posto_127 = df.loc[df.cod_posto == 127, ['data', 'vazao_natural']].set_index('data')

        # Simplício ( Se (127) <= 430: max(0; (127) - 90)
        #             Se (127) > 430: 340
        posto = 126
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_127.vazao_natural.apply(lambda x: max(0, x-90) if x <= 430 else 340)).reset_index(drop=True)
        df = df.append(posto_df)

        # Nilo Peçanha ( Min [(316) ; 144 ] )
        posto = 131
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_316.vazao_natural.apply(lambda x: min(x, 144))).reset_index(drop=True)
        df = df.append(posto_df)

        posto_131 = df.loc[df.cod_posto == 131, ['data', 'vazao_natural']].set_index('data')

        # Lajes ( (202) + Min [ (201) ; 25 ] )
        posto = 132
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_202.vazao_natural + posto_201.vazao_natural.apply(lambda x: min(x,25))).reset_index(drop=True)
        df = df.append(posto_df)

        posto_132 = df.loc[df.cod_posto == 132, ['data', 'vazao_natural']].set_index('data')

        # Fontes ( (Se (132) < 17: (132) ; Se não: 17) + Min [ (316)-(131) ; 34 ] )
        posto = 303
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_132.vazao_natural.apply(lambda x: x if x<17 else 17)
        + (posto_316 - posto_131).vazao_natural.apply(lambda x: min(x,34))).reset_index(drop=True)
        df = df.append(posto_df)

        posto_303 = df.loc[df.cod_posto == 303, ['data', 'vazao_natural']].set_index('data')

        # Pereira Passos ( (303) + (131) )
        posto = 306
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_303 + posto_131).reset_index(drop=True)
        df = df.append(posto_df)

        # --------------------------------------------------------------
        # ----------------------- BACIA DO TIETÊ -----------------------
        # --------------------------------------------------------------

        posto_34 = df.loc[df.cod_posto == 34, ['data', 'vazao_natural']].set_index('data')
        posto_117 = df.loc[df.cod_posto == 117, ['data', 'vazao_natural']].set_index('data')
        posto_118 = df.loc[df.cod_posto == 118, ['data', 'vazao_natural']].set_index('data')
        posto_161 = df.loc[df.cod_posto == 161, ['data', 'vazao_natural']].set_index('data')
        posto_237 = df.loc[df.cod_posto == 237, ['data', 'vazao_natural']].set_index('data')
        posto_238 = df.loc[df.cod_posto == 238, ['data', 'vazao_natural']].set_index('data')
        posto_239 = df.loc[df.cod_posto == 239, ['data', 'vazao_natural']].set_index('data')
        posto_240 = df.loc[df.cod_posto == 240, ['data', 'vazao_natural']].set_index('data')
        posto_242 = df.loc[df.cod_posto == 242, ['data', 'vazao_natural']].set_index('data')
        posto_243 = df.loc[df.cod_posto == 243, ['data', 'vazao_natural']].set_index('data')
        posto_245 = df.loc[df.cod_posto == 245, ['data', 'vazao_natural']].set_index('data')
        posto_246 = df.loc[df.cod_posto == 246, ['data', 'vazao_natural']].set_index('data')
        posto_266 = df.loc[df.cod_posto == 266, ['data', 'vazao_natural']].set_index('data')

        # Traição ( (117) + (118) )
        posto = 104
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_117 + posto_118).reset_index(drop=True)
        df = df.append(posto_df)
    
        # Pedreira ( (118) )
        posto = 109
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = posto_118.reset_index(drop=True)
        df = df.append(posto_df)

        # Billings + Pedras ( ((118) - 0.185) / 0.8103 )
        posto = 119
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = ((posto_118 - 0.185) / 0.8103).reset_index(drop=True)
        df = df.append(posto_df)

        posto_119 = df.loc[df.cod_posto == 119, ['data', 'vazao_natural']].set_index('data')

        # Pedras ( (119) - (118) )
        posto = 116
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_119 - posto_118).reset_index(drop=True)
        df = df.append(posto_df)

        posto_116 = df.loc[df.cod_posto == 116, ['data', 'vazao_natural']].set_index('data')

        # Ponte Nova ( (161) * 0.0924 )
        posto = 160
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_161 * 0.0924).reset_index(drop=True)
        df = df.append(posto_df)

        # Henry Borden ( (116) + 0.1[(161) - (117) - (118)] + (117) + (118) )
        posto = 318
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_116 + (0.1 * (posto_161 - posto_117 - posto_118)) + posto_117 + posto_118).reset_index(drop=True)
        df = df.append(posto_df)

        # Billings + Pedras v2 ( (119) )
        posto = 320
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = posto_119.reset_index(drop=True)
        df = df.append(posto_df)

        # Barra Bonita ( (237) - 0,1*[(161) – (117) – (118)] – (117) – (118) )
        posto = 37
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_237 - (0.1 * (posto_161 - posto_117 - posto_118)) - posto_117 - posto_118).reset_index(drop=True)
        df = df.append(posto_df)

        # Bariri ( (238) - 0,1*[(161) – (117) – (118)] – (117) – (118) )
        posto = 38
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_238 - (0.1 * (posto_161 - posto_117 - posto_118)) - posto_117 - posto_118).reset_index(drop=True)
        df = df.append(posto_df)

        # Ibitinga ( (239) - 0,1*[(161) – (117) – (118)] – (117) – (118) )
        posto = 39
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_239 - (0.1 * (posto_161 - posto_117 - posto_118)) - posto_117 - posto_118).reset_index(drop=True)
        df = df.append(posto_df)

        # Promissão ( (240) - 0,1*[(161) – (117) – (118)] – (117) – (118) )
        posto = 40
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_240 - (0.1 * (posto_161 - posto_117 - posto_118)) - posto_117 - posto_118).reset_index(drop=True)
        df = df.append(posto_df)

        # Nova Avanhandava ( (242) - 0,1*[(161) – (117) – (118)] – (117) – (118) )
        posto = 42
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_242 - (0.1 * (posto_161 - posto_117 - posto_118)) - posto_117 - posto_118).reset_index(drop=True)
        df = df.append(posto_df)

        # Três Irmãos ( (243) - 0,1*[(161) – (117) – (118)] – (117) – (118) )
        posto = 43
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_243 - (0.1 * (posto_161 - posto_117 - posto_118)) - posto_117 - posto_118).reset_index(drop=True)
        df = df.append(posto_df)

        # Ilha Solteira Equivalente ( (243) + (34) )
        posto = 244
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_243 + posto_34).reset_index(drop=True)
        df = df.append(posto_df)

        posto_244 = df.loc[df.cod_posto == 244, ['data', 'vazao_natural']].set_index('data')

        # Ilha Solteira Artificial ( (244) - 0,1*[(161) – (117) – (118)] – (117) – (118) )
        posto = 44
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_244 - (0.1 * (posto_161 - posto_117 - posto_118)) - posto_117 - posto_118).reset_index(drop=True)
        df = df.append(posto_df)

        # Jupiá ( (245) - 0,1*[(161) – (117) – (118)] – (117) – (118) )
        posto = 45
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_245 - (0.1 * (posto_161 - posto_117 - posto_118)) - posto_117 - posto_118).reset_index(drop=True)
        df = df.append(posto_df)

        # Porto Primavera ( (246) - 0,1*[(161) – (117) – (118)] – (117) – (118) )
        posto = 46
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_246 - (0.1 * (posto_161 - posto_117 - posto_118)) - posto_117 - posto_118).reset_index(drop=True)
        df = df.append(posto_df)

        # Itaipu ( (266) - 0,1*[(161) – (117) – (118)] – (117) – (118) )
        posto = 66
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_266 - (0.1 * (posto_161 - posto_117 - posto_118)) - posto_117 - posto_118).reset_index(drop=True)
        df = df.append(posto_df)

        # --------------------------------------------------------------
        # --------------- COMPLEXO PAULO AFONSO - MOXOTÓ ---------------
        # --------------------------------------------------------------

        posto_173 = df.loc[df.cod_posto == 173, ['data', 'vazao_natural']].set_index('data')

        # Paulo Afonso ( (173) )
        posto = 175
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_173).reset_index(drop=True)
        df = df.append(posto_df)

        # Moxotó ( (173) )
        posto = 176
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_173).reset_index(drop=True)
        df = df.append(posto_df)

        # --------------------------------------------------------------
        # --------------- DESVIO RIO JORDÃO - RIO IGUAÇU ---------------
        # --------------------------------------------------------------

        posto_73 = df.loc[df.cod_posto == 73, ['data', 'vazao_natural']].set_index('data')
        posto_76 = df.loc[df.cod_posto == 76, ['data', 'vazao_natural']].set_index('data')

        # Segredo ( (76) + min [ 173.5 ; (73) – 10 ] )
        posto = 75
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['vazao_natural'] = (posto_76.vazao_natural + posto_73.vazao_natural.apply(lambda x: min(173.5, x-10))).reset_index(drop=True)
        df = df.append(posto_df)

        # --------------------------------------------------------------
        # ---------------------- DESVIO RIO XINGU ----------------------
        # --------------------------------------------------------------

        posto_288 = df.loc[df.cod_posto == 288, ['data', 'vazao_natural']].set_index('data')
        
        # Belo Monte ( Se (288) < (mês hidrog B): 0
        #              Se (288) > (mês hidrog B) + 13.900: 13.900
        #              Se não: (288) - (mês hidrog B) )

        def get_posto_292(p_288, hidr_b):
            if p_288 < hidr_b:
                return 0
            elif p_288 > (hidr_b + 13900):
                return 13900
            else:
                return p_288 - hidr_b
        hidrograma = Acomph.get_hidrograma(janela_data, connection, tipo='b')
        merge = hidrograma.merge(posto_288, left_index=True, right_index=True)

        posto = 292
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['ena'] = (merge.apply(
            lambda x: get_posto_292(x.vazao_natural, x.b) * 0.8032, axis=1)
            ).reset_index(drop=True)
        df = df.append(posto_df)

        # Pimental ( Se (288) < (mês hidrog B): 0
        #            Se (288) > (mês hidrog B) + 13.900: 13.900
        #            Se não: (288) - (mês hidrog B) )
        posto = 302
        posto_df = pd.DataFrame({'cod_posto': posto, 'data': janela_data})
        posto_df['ena'] = ((posto_288.vazao_natural - merge.apply(
            lambda x: get_posto_292(x.vazao_natural, x.b), axis=1)) * 0.1163
            ).reset_index(drop=True)
        df = df.append(posto_df)
        
        return df

    '''
    get_hidrograma(datas,connection,tipo) : recebe um período determinado e retorna os valores
    do hidrograma do ano atual que consta no banco de dados
    '''
    def get_hidrograma(datas, connection, tipo='b'):
        table_name = 'hidrograma_bm'

        min_data = pd.to_datetime(min(datas)).strftime('%Y-%m-%d')
        max_data = pd.to_datetime(max(datas)).strftime('%Y-%m-%d')
        sql_command = 'SELECT {0},{1} FROM {2} WHERE data BETWEEN "{3}" AND "{4}";'.format('data',tipo,table_name, min_data,max_data)
        valores = pd.DataFrame(connection.execute(sql_command).fetchall(), columns=['data',tipo]).set_index('data')
        return valores
